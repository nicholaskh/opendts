package register

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"git.100tal.com/wangxiao_jichujiagou_common/dts/config"
	etcdclient "git.100tal.com/wangxiao_jichujiagou_common/dts/etcd"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/log"
)

type Register struct {
	info   *ReplicatorInfo
	client *etcdclient.EtcdClient
	prefix string
}

func NewRegister(cfg *config.Etcd) (*Register, error) {
	c, err := etcdclient.New(cfg.Addr, time.Minute, cfg.Username, cfg.Password, cfg.Root)
	if err != nil {
		log.Fatal("create etcdclient to %s failed, %v", cfg.Addr, err)
		return nil, err
	}

	return &Register{
		client: c,
		prefix: c.BasePrefix(),
	}, nil
}

// Close close store
func (r *Register) Close() error {
	return r.client.Close()
}

func (r *Register) ReplicatorBase() string {
	return filepath.Join(r.prefix, "instances")
}

func (r *Register) ReplicatorPath(token string) string {
	return filepath.Join(r.prefix, "instances", fmt.Sprintf("%s", token))
}

func (r *Register) CreateReplicator(info *ReplicatorInfo) error {
	return r.client.Update(r.ReplicatorPath(info.Addr), info.Encode())
}

// DeleteProxy delete proxy path
func (r *Register) DeleteReplicator(token string) error {
	return r.client.Delete(r.ReplicatorPath(r.info.Addr))
}

// ListProxyMonitorMetrics list proxies in proxy register path
func (r *Register) ListInstances() ([]string, error) {
	return r.client.List(r.ReplicatorBase())
}

// ListProxyMonitorMetrics list proxies in proxy register path
func (r *Register) ListReplicatorInfo() (map[string]*ReplicatorInfo, error) {
	files, err := r.client.List(r.ReplicatorBase())
	if err != nil {
		return nil, err
	}
	replicator := make(map[string]*ReplicatorInfo)
	for _, path := range files {
		b, err := r.client.Read(path)
		if err != nil {
			return nil, err
		}
		rr := &ReplicatorInfo{}
		if err := JSONDecode(rr, b); err != nil {
			return nil, err
		}
		replicator[rr.Addr] = rr
	}
	return replicator, nil
}

func (r *Register) RegisterReplicator(addr string) error {
	r.info = &ReplicatorInfo{
		Addr:      addr,
		StartTime: time.Now().String(),
		Pid:       os.Getpid(),
	}
	r.info.Pwd, _ = os.Getwd()

	if err := r.CreateReplicator(r.info); err != nil {
		return err
	}
	return nil
}

func (r *Register) UnregisterReplicator() error {
	if err := r.DeleteReplicator(r.info.Addr); err != nil {
		return err
	}
	return nil
}
