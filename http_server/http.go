package http_server

import (
	"context"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"sync"

	bm "github.com/go-kratos/kratos/pkg/net/http/blademaster"

	"github.com/nicholaskh/opendts"
	"github.com/nicholaskh/opendts/register"
)

type HttpServer struct {
	*bm.Engine
	// FIXME use conn pool
	dbClient       *replicator.DBClient
	stats          *replicator.Stats
	port           int
	mu             sync.Mutex
	taskCancel     map[int]context.CancelFunc
	taskReplicator map[int]*replicator.Replicator
	r              *register.Register
}

// New new a bm server.
func New(cfg *bm.ServerConfig, dbClient *replicator.DBClient, stats *replicator.Stats, r *register.Register) (*HttpServer, error) {
	parts := strings.Split(cfg.Addr, ":")
	portStr := parts[len(parts)-1]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}
	engine := bm.DefaultServer(cfg)

	server := HttpServer{
		Engine:         engine,
		dbClient:       dbClient,
		stats:          stats,
		port:           port,
		taskCancel:     make(map[int]context.CancelFunc),
		taskReplicator: make(map[int]*replicator.Replicator),
		r:              r,
	}

	server.initRouter()
	if err := server.Start(); err != nil {
		return nil, err
	}
	return &server, nil
}
