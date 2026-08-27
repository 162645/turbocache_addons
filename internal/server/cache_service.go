package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"turbocache/api/generated/cachepb"
	"turbocache/internal/cache/engine"
	"turbocache/internal/cache/storage"
	"turbocache/internal/cache/value"
	"turbocache/internal/cluster/hash"
	"turbocache/internal/utils/logger"
)

// CacheService implements cachepb.CacheServiceServer.
//
// IMPORTANT:
//   - Get: supports "smart forwarding" to the node responsible for the key.
//   - Set/Delete: only mutate local L1. Replication is orchestrated by clients
//     (e.g. engine.L2Cache or turbocachectl) to avoid replication storms.
//
// This design matches the existing engine.L2Cache behavior:
//   - L2Cache.Set() replicates to remote nodes by calling server.Set() on each node.
//   - Therefore server.Set() must NOT replicate further.
type CacheService struct {
	cache            *engine.L1Cache
	multiCache       *engine.MultiLevelCache
	l2               *engine.L2Cache
	nodeID           string
	ring             *hash.ConsistentHash
	conns            *ConnManager
	maxForwardDepth  int
	promoteOnForward bool

	metrics *Metrics
	logger  logger.Logger

	cachepb.UnimplementedCacheServiceServer
}

type CacheServiceOptions struct {
	Cache            *engine.L1Cache
	MultiCache       *engine.MultiLevelCache
	L2               *engine.L2Cache
	NodeID           string
	Ring             *hash.ConsistentHash
	Conns            *ConnManager
	MaxForwardDepth  int
	PromoteOnForward bool
	Metrics          *Metrics
}

func NewCacheService(opts CacheServiceOptions) (*CacheService, error) {
	if opts.Cache == nil {
		return nil, fmt.Errorf("cache is required")
	}
	if opts.NodeID == "" {
		return nil, fmt.Errorf("nodeID is required")
	}
	if opts.Ring == nil {
		return nil, fmt.Errorf("hash ring is required")
	}
	if opts.Conns == nil {
		opts.Conns = NewConnManager(3 * time.Second)
	}
	if opts.MaxForwardDepth <= 0 {
		opts.MaxForwardDepth = 3
	}

	return &CacheService{
		cache:            opts.Cache,
		multiCache:       opts.MultiCache,
		l2:               opts.L2,
		nodeID:           opts.NodeID,
		ring:             opts.Ring,
		conns:            opts.Conns,
		maxForwardDepth:  opts.MaxForwardDepth,
		promoteOnForward: opts.PromoteOnForward,
		metrics:          opts.Metrics,
		logger:           logger.WithField("component", "grpc_cache_service"),
	}, nil
}

func (s *CacheService) Get(ctx context.Context, req *cachepb.GetRequest) (*cachepb.GetResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	key := strings.TrimSpace(req.Key)
	if key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}

	if s.metrics != nil {
		s.metrics.IncGet("total")
	}

	// The production node path is the composed L1 -> L2 pipeline. The legacy
	// forwarding path below remains only for isolated service unit tests.
	if s.multiCache != nil {
		entry, err := s.multiCache.Get(ctx, key)
		if err != nil || entry == nil {
			if s.metrics != nil {
				s.metrics.IncGet("miss")
				s.metrics.IncMiss("overall")
			}
			return &cachepb.GetResponse{Found: false}, nil
		}
		if s.metrics != nil {
			s.metrics.IncGet("hit")
			s.metrics.IncHit("overall")
		}
		return &cachepb.GetResponse{Found: true, Value: entry.Value.ByteSlice()}, nil
	}

	// 1) Try local L1.
	entry, err := s.cache.Get(ctx, key)
	if err == nil && entry != nil {
		if s.metrics != nil {
			s.metrics.IncGet("local_hit")
			s.metrics.IncHit("l1")
		}
		return &cachepb.GetResponse{
			Found: true,
			Value: entry.Value.ByteSlice(),
		}, nil
	}

	if err != nil && !storage.IsNotFound(err) {
		// unexpected local error
		if s.metrics != nil {
			s.metrics.IncGet("error")
		}
		return nil, status.Errorf(codes.Internal, "l1 get failed: %v", err)
	}

	if s.metrics != nil {
		s.metrics.IncMiss("l1")
	}

	// 2) Decide responsible node.
	node := s.ring.GetNode(key)
	if node == nil {
		if s.metrics != nil {
			s.metrics.IncGet("miss")
			s.metrics.IncMiss("overall")
		}
		return &cachepb.GetResponse{Found: false}, nil
	}

	if node.ID == s.nodeID {
		// We are responsible and L1 missed => not found.
		if s.metrics != nil {
			s.metrics.IncGet("miss")
			s.metrics.IncMiss("overall")
		}
		return &cachepb.GetResponse{Found: false}, nil
	}

	// 3) Forward.
	depth, source := parseDepthAndSource(req)

	// loop protection
	if source != "" && source == s.nodeID && depth > 0 {
		return nil, status.Error(codes.Aborted, "forward loop detected")
	}
	if depth >= s.maxForwardDepth {
		return nil, status.Error(codes.ResourceExhausted, "max forward depth reached")
	}

	fwdReq := cloneGetRequest(req)
	if fwdReq.Metadata == nil {
		fwdReq.Metadata = &cachepb.RequestMetadata{}
	}
	if fwdReq.Metadata.RequestId == "" {
		fwdReq.Metadata.RequestId = newRequestID()
	}
	fwdReq.Metadata.ClientId = s.nodeID
	if fwdReq.Metadata.Headers == nil {
		fwdReq.Metadata.Headers = map[string]string{}
	}
	// increment depth
	fwdReq.Metadata.Headers["X-TurboCache-Depth"] = strconv.Itoa(depth + 1)
	if source == "" {
		fwdReq.Metadata.Headers["X-TurboCache-Source"] = s.nodeID
	}
	fwdReq.Metadata.Headers["X-TurboCache-Forwarder"] = s.nodeID

	conn, err := s.conns.Get(ctx, node.Address)
	if err != nil {
		if s.metrics != nil {
			s.metrics.IncGet("error")
			s.metrics.IncForward("error")
		}
		return nil, status.Errorf(codes.Unavailable, "dial %s failed: %v", node.Address, err)
	}

	client := cachepb.NewCacheServiceClient(conn)

	if s.metrics != nil {
		s.metrics.IncForward("ok")
	}

	resp, err := client.Get(ctx, fwdReq)
	if err != nil {
		if s.metrics != nil {
			s.metrics.IncGet("error")
		}
		return nil, err
	}

	if resp != nil && resp.Found && s.promoteOnForward {
		// Best-effort promote to local L1 to reduce future hop.
		vv := value.NewByteView(resp.Value)
		promoteEntry := &value.Entry{Value: vv}
		go func() {
			_ = s.cache.Set(context.Background(), key, promoteEntry)
		}()
	}

	if resp != nil && resp.Found {
		if s.metrics != nil {
			s.metrics.IncGet("forward_hit")
			s.metrics.IncHit("forward")
		}
	} else {
		if s.metrics != nil {
			s.metrics.IncGet("miss")
			s.metrics.IncMiss("overall")
		}
	}

	// return as-is
	return resp, nil
}

func (s *CacheService) Set(ctx context.Context, req *cachepb.SetRequest) (*cachepb.SetResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	key := strings.TrimSpace(req.Key)
	if key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}

	// Create entry with TTL.
	ttl := time.Duration(0)
	if req.Ttl != nil {
		ttl = req.Ttl.AsDuration()
	}

	entry := value.NewEntry(key, value.NewByteView(req.Value), ttl)

	var err error
	if isInternalReplica(req.Metadata) || s.multiCache == nil {
		err = s.cache.Set(ctx, key, entry)
	} else {
		err = s.multiCache.Set(ctx, key, entry)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cache set failed: %v", err)
	}

	return &cachepb.SetResponse{Success: true}, nil
}

func (s *CacheService) Delete(ctx context.Context, req *cachepb.DeleteRequest) (*cachepb.DeleteResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	key := strings.TrimSpace(req.Key)
	if key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}

	var err error
	if isInternalReplica(req.Metadata) || s.multiCache == nil {
		err = s.cache.Delete(ctx, key)
	} else {
		err = s.multiCache.Delete(ctx, key)
	}
	if err != nil && !storage.IsNotFound(err) {
		return nil, status.Errorf(codes.Internal, "l1 delete failed: %v", err)
	}

	return &cachepb.DeleteResponse{Success: true}, nil
}

// BatchSet carries internal Bloom deltas/snapshots over the existing protobuf
// contract. Public batch cache operations remain intentionally unsupported.
func (s *CacheService) BatchSet(ctx context.Context, req *cachepb.BatchSetRequest) (*cachepb.BatchSetResponse, error) {
	op := bloomOperation(req.GetMetadata())
	if op == "" || s.l2 == nil {
		return nil, status.Error(codes.Unimplemented, "public BatchSet is not implemented")
	}
	switch op {
	case engine.BloomDeltaOperation:
		keys := make([]string, 0, len(req.Items))
		for _, item := range req.Items {
			if item != nil && item.Key != "" {
				keys = append(keys, item.Key)
			}
		}
		s.l2.ObserveBloomKeys(keys)
	case engine.BloomSnapshotOperation:
		if len(req.Items) == 0 || req.Items[0] == nil {
			return nil, status.Error(codes.InvalidArgument, "missing bloom snapshot")
		}
		if err := s.l2.ApplyBloomSnapshot(req.Items[0].Value); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "apply bloom snapshot: %v", err)
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "unknown bloom operation")
	}
	return &cachepb.BatchSetResponse{}, nil
}

func (s *CacheService) BatchGet(ctx context.Context, req *cachepb.BatchGetRequest) (*cachepb.BatchGetResponse, error) {
	op := bloomOperation(req.GetMetadata())
	if op == "" || s.l2 == nil {
		return nil, status.Error(codes.Unimplemented, "public BatchGet is not implemented")
	}
	switch op {
	case engine.BloomSnapshotOperation:
		snapshot, err := s.l2.BloomSnapshot()
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "bloom snapshot: %v", err)
		}
		return &cachepb.BatchGetResponse{Results: []*cachepb.GetResult{{Key: "__cluster_bloom_snapshot__", Value: snapshot, Found: true}}}, nil
	case engine.BloomLocalKeysOperation:
		keys, err := s.cache.Keys(ctx)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list local keys: %v", err)
		}
		results := make([]*cachepb.GetResult, 0, len(keys))
		for _, key := range keys {
			results = append(results, &cachepb.GetResult{Key: key, Found: true})
		}
		return &cachepb.BatchGetResponse{Results: results}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "unknown bloom operation")
	}
}

func bloomOperation(md *cachepb.RequestMetadata) string {
	if md == nil || md.Headers == nil {
		return ""
	}
	return md.Headers[engine.BloomInternalHeader]
}

func isInternalReplica(md *cachepb.RequestMetadata) bool {
	return md != nil && md.Headers != nil && md.Headers["X-TurboCache-Internal"] == "replica"
}

// --- helpers ---

func parseDepthAndSource(req *cachepb.GetRequest) (depth int, source string) {
	if req == nil || req.Metadata == nil || req.Metadata.Headers == nil {
		return 0, ""
	}

	if v, ok := req.Metadata.Headers["X-TurboCache-Depth"]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			depth = n
		}
	}
	source = req.Metadata.Headers["X-TurboCache-Source"]
	return depth, source
}

func cloneGetRequest(req *cachepb.GetRequest) *cachepb.GetRequest {
	if req == nil {
		return &cachepb.GetRequest{}
	}

	out := &cachepb.GetRequest{
		Key:   req.Key,
		Group: req.Group,
	}

	if req.Metadata != nil {
		md := &cachepb.RequestMetadata{
			RequestId: req.Metadata.RequestId,
			ClientId:  req.Metadata.ClientId,
		}

		if req.Metadata.Headers != nil {
			md.Headers = make(map[string]string, len(req.Metadata.Headers))
			for k, v := range req.Metadata.Headers {
				md.Headers[k] = v
			}
		}
		out.Metadata = md
	}

	return out
}

func newRequestID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// fallback
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
