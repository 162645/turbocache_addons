package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"

	"turbocache/api/generated/cachepb"
	"turbocache/internal/cluster/discovery"
	"turbocache/internal/cluster/hash"
	"turbocache/internal/config"
)

// turbocachectl is a tiny CLI to test a turbocache cluster.
//
// Examples:
//
//	turbocachectl set --etcd http://localhost:2379 --ttl 30s foo bar
//	turbocachectl get --etcd http://localhost:2379 foo
//	turbocachectl del --etcd http://localhost:2379 foo
//
// You can also target a fixed node directly:
//
//	turbocachectl get --addr localhost:7071 foo
func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	// Global flags
	var (
		etcdEndpoints = flag.String("etcd", envOr("TC_ETCD_ENDPOINTS", ""), "comma-separated etcd endpoints")
		registerPath  = flag.String("etcd-path", envOr("TC_ETCD_PATH", "/turbocache/nodes"), "etcd register path")
		addr          = flag.String("addr", envOr("TC_ADDR", ""), "direct grpc addr (host:port), skip etcd")
		timeout       = flag.Duration("timeout", 3*time.Second, "rpc timeout")

		hashReplicas      = flag.Int("hash-replicas", 128, "virtual nodes per weight")
		replicationFactor = flag.Int("rf", 2, "replication factor (set/del)")
		consistency       = flag.String("consistency", "strong", "strong|eventual")

		ttlStr = flag.String("ttl", "", "ttl for set, e.g. 30s")
	)

	// Parse subcommand manually:
	cmd := os.Args[1]
	// Reset flag.CommandLine to parse after cmd.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flag.CommandLine.StringVar(etcdEndpoints, "etcd", *etcdEndpoints, "")
	flag.CommandLine.StringVar(registerPath, "etcd-path", *registerPath, "")
	flag.CommandLine.StringVar(addr, "addr", *addr, "")
	flag.CommandLine.DurationVar(timeout, "timeout", *timeout, "")
	flag.CommandLine.IntVar(hashReplicas, "hash-replicas", *hashReplicas, "")
	flag.CommandLine.IntVar(replicationFactor, "rf", *replicationFactor, "")
	flag.CommandLine.StringVar(consistency, "consistency", *consistency, "")
	flag.CommandLine.StringVar(ttlStr, "ttl", *ttlStr, "")

	_ = flag.CommandLine.Parse(os.Args[2:])
	args := flag.CommandLine.Args()

	// load nodes
	nodes, err := loadNodes(*addr, *etcdEndpoints, *registerPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "load nodes failed:", err)
		os.Exit(1)
	}
	if len(nodes) == 0 {
		fmt.Fprintln(os.Stderr, "no nodes available")
		os.Exit(1)
	}

	// ring
	ring := hash.NewConsistentHash(*hashReplicas, nil)
	for _, n := range nodes {
		ring.AddNode(n)
	}

	// TTL
	var ttl time.Duration
	if *ttlStr != "" {
		d, err := time.ParseDuration(*ttlStr)
		if err != nil {
			fmt.Fprintln(os.Stderr, "invalid ttl:", err)
			os.Exit(2)
		}
		ttl = d
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch cmd {
	case "get":
		if len(args) != 1 {
			usage()
			os.Exit(2)
		}
		key := args[0]
		val, found, err := doGet(ctx, ring, key)
		if err != nil {
			fmt.Fprintln(os.Stderr, "get error:", err)
			os.Exit(1)
		}
		if !found {
			fmt.Println("(not found)")
			return
		}
		fmt.Println(val)

	case "set":
		if len(args) < 2 {
			usage()
			os.Exit(2)
		}
		key := args[0]
		value := strings.Join(args[1:], " ")

		err := doSet(ctx, ring, key, []byte(value), ttl, *replicationFactor, strings.ToLower(*consistency))
		if err != nil {
			fmt.Fprintln(os.Stderr, "set error:", err)
			os.Exit(1)
		}
		fmt.Println("OK")

	case "del", "delete":
		if len(args) != 1 {
			usage()
			os.Exit(2)
		}
		key := args[0]
		err := doDelete(ctx, ring, key, *replicationFactor, strings.ToLower(*consistency))
		if err != nil {
			fmt.Fprintln(os.Stderr, "delete error:", err)
			os.Exit(1)
		}
		fmt.Println("OK")

	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Print(`Usage:
	  turbocachectl <cmd> [flags] [args]

Commands:
  get <key>
	  set [flags] <key> <value>
  del <key>

Global flags:
  --etcd <endpoints>        comma-separated etcd endpoints
  --etcd-path <path>        default: /turbocache/nodes
  --addr <host:port>        bypass etcd, treat as single node
  --rf <n>                  replication factor for set/del
  --consistency strong|eventual
  --hash-replicas <n>
  --timeout <duration>
`)
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func loadNodes(addr, etcdEndpoints, registerPath string) ([]*hash.Node, error) {
	if addr != "" {
		return []*hash.Node{{ID: "single", Address: addr, Weight: 1}}, nil
	}

	eps := splitComma(etcdEndpoints)
	if len(eps) == 0 {
		return nil, fmt.Errorf("--etcd is required (or use --addr)")
	}

	discCfg := &config.DiscoveryConfig{Endpoints: eps, RegisterPath: registerPath}
	disc, err := discovery.NewEtcdDiscovery(discCfg)
	if err != nil {
		return nil, err
	}
	defer disc.Close()

	m, err := disc.GetNodes(context.Background())
	if err != nil {
		return nil, err
	}

	out := make([]*hash.Node, 0, len(m))
	for _, n := range m {
		if n == nil {
			continue
		}
		out = append(out, &hash.Node{ID: n.ID, Address: n.Address, Weight: n.Weight, Metadata: n.Metadata})
	}
	return out, nil
}

func splitComma(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func dial(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
}

func doGet(ctx context.Context, ring *hash.ConsistentHash, key string) (string, bool, error) {
	nodes := ring.GetNodes(key, 3) // try a few replicas for robustness
	if len(nodes) == 0 {
		return "", false, nil
	}

	var lastErr error
	for _, n := range nodes {
		conn, err := dial(ctx, n.Address)
		if err != nil {
			lastErr = err
			continue
		}
		client := cachepb.NewCacheServiceClient(conn)
		resp, err := client.Get(ctx, &cachepb.GetRequest{Key: key})
		_ = conn.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if resp != nil && resp.Found {
			return string(resp.Value), true, nil
		}
		// not found on this node, try next
	}

	if lastErr != nil {
		return "", false, lastErr
	}
	return "", false, nil
}

func doSet(ctx context.Context, ring *hash.ConsistentHash, key string, val []byte, ttl time.Duration, rf int, consistency string) error {
	nodes := ring.GetNodes(key, rf)
	if len(nodes) == 0 {
		return fmt.Errorf("no nodes")
	}

	req := &cachepb.SetRequest{Key: key, Value: val}
	if ttl > 0 {
		req.Ttl = durationpb.New(ttl)
	}

	switch consistency {
	case "eventual":
		// fire-and-forget to all replicas
		for _, n := range nodes {
			n := n
			go func() {
				cctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				_ = callSet(cctx, n.Address, req)
			}()
		}
		return nil
	default:
		// strong: wait all
		var wg sync.WaitGroup
		errCh := make(chan error, len(nodes))
		for _, n := range nodes {
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				if err := callSet(ctx, addr, req); err != nil {
					errCh <- err
				}
			}(n.Address)
		}
		wg.Wait()
		close(errCh)
		if len(errCh) > 0 {
			return <-errCh
		}
		return nil
	}
}

func callSet(ctx context.Context, addr string, req *cachepb.SetRequest) error {
	conn, err := dial(ctx, addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := cachepb.NewCacheServiceClient(conn)
	resp, err := client.Set(ctx, req)
	if err != nil {
		return err
	}
	if resp == nil || !resp.Success {
		return fmt.Errorf("set failed")
	}
	return nil
}

func doDelete(ctx context.Context, ring *hash.ConsistentHash, key string, rf int, consistency string) error {
	nodes := ring.GetNodes(key, rf)
	if len(nodes) == 0 {
		return fmt.Errorf("no nodes")
	}

	req := &cachepb.DeleteRequest{Key: key}

	switch consistency {
	case "eventual":
		for _, n := range nodes {
			n := n
			go func() {
				cctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				_ = callDelete(cctx, n.Address, req)
			}()
		}
		return nil
	default:
		var wg sync.WaitGroup
		errCh := make(chan error, len(nodes))
		for _, n := range nodes {
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				if err := callDelete(ctx, addr, req); err != nil {
					errCh <- err
				}
			}(n.Address)
		}
		wg.Wait()
		close(errCh)
		if len(errCh) > 0 {
			return <-errCh
		}
		return nil
	}
}

func callDelete(ctx context.Context, addr string, req *cachepb.DeleteRequest) error {
	conn, err := dial(ctx, addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := cachepb.NewCacheServiceClient(conn)
	resp, err := client.Delete(ctx, req)
	if err != nil {
		return err
	}
	if resp == nil || !resp.Success {
		return fmt.Errorf("delete failed")
	}
	return nil
}
