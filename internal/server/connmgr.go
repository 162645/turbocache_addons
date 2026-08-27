package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ConnManager is a tiny, safe gRPC connection cache.
//
// We intentionally keep it simple:
//   - one connection per address
//   - Dial on-demand with timeout
//   - Close all on shutdown
//
// This avoids depending on any specific pool implementation while still
// preventing excessive Dial() calls when a node forwards requests.
type ConnManager struct {
	mu          sync.Mutex
	conns       map[string]*grpc.ClientConn
	dialTimeout time.Duration
}

func NewConnManager(dialTimeout time.Duration) *ConnManager {
	if dialTimeout <= 0 {
		dialTimeout = 3 * time.Second
	}
	return &ConnManager{
		conns:       make(map[string]*grpc.ClientConn),
		dialTimeout: dialTimeout,
	}
}

func (m *ConnManager) Get(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	if addr == "" {
		return nil, fmt.Errorf("empty address")
	}

	m.mu.Lock()
	if conn, ok := m.conns[addr]; ok && conn != nil {
		m.mu.Unlock()
		return conn, nil
	}
	m.mu.Unlock()

	dialCtx := ctx
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		dialCtx, cancel = context.WithTimeout(ctx, m.dialTimeout)
	} else {
		// still cap the dial to a reasonable bound in case ctx deadline is huge
		dialCtx, cancel = context.WithTimeout(ctx, m.dialTimeout)
	}
	defer cancel()

	conn, err := grpc.DialContext(
		dialCtx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	// if another goroutine already stored a conn, prefer the existing one
	if existing, ok := m.conns[addr]; ok && existing != nil {
		m.mu.Unlock()
		_ = conn.Close()
		return existing, nil
	}
	m.conns[addr] = conn
	m.mu.Unlock()

	return conn, nil
}

func (m *ConnManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for addr, conn := range m.conns {
		if conn == nil {
			continue
		}
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close conn %s: %w", addr, err)
		}
	}
	m.conns = make(map[string]*grpc.ClientConn)
	return firstErr
}
