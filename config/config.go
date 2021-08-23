package config

import (
	"errors"
	"fmt"
	"time"

	"github.com/go-ini/ini"
	bm "github.com/go-kratos/kratos/pkg/net/http/blademaster"
	xtime "github.com/go-kratos/kratos/pkg/time"

	"git.100tal.com/wangxiao_jichujiagou_common/dts/mysql"
)

var Conf *ReplicatorConfig

type Log struct {
	Output   string `ini:"output"`
	Path     string `ini:"path"`
	Filename string `ini:"filename"`
	Level    string `ini:"level"`
	Service  string `ini:"service"`
	Format   string `ini:"format"`
}

type Etcd struct {
	Addr     string `ini:"addr"`
	Username string `ini:"username"`
	Password string `ini:"password"`
	Root     string `ini:"root"`
}

type Mysql struct {
	Host             string `ini:"host"`
	Port             int    `ini:"port"`
	Username         string `ini:"username"`
	Password         string `ini:"password"`
	DB               string `ini:"db"`
	Charset          string `ini:"charset"`
	ConnectTimeoutMs uint64 `ini:"connect_timeout_ms"`
}

type Filter struct {
	Match  []string `ini:"match,,allowshadow"`
	Filter []string `ini:"filter,,allowshadow"`
}

type BM struct {
	Addr    string `ini:"addr"`
	Timeout string `ini:"timeout"`
}

type ReplicatorConfig struct {
	Cluster string `ini:"cluster"`
	Log     `ini:"log"`
	Etcd    `ini:"etcd"`
	Storage Mysql `ini:"storage"`

	BmIni BM `ini:"bm"`
	BM    *bm.ServerConfig
}

func LoadConfig(configPath string) (*ReplicatorConfig, error) {
	cfg, err := ini.ShadowLoad(configPath)
	if err != nil {
		return nil, err
	}
	rc := ReplicatorConfig{}
	err = cfg.MapTo(&rc)
	if err != nil {
		return nil, err
	}

	if rc.Cluster == "" {
		return nil, errors.New("unspecified cluster name")
	}

	if rc.Log.Output == "" {
		rc.Log.Output = "file"
	}
	if rc.Log.Path == "" {
		rc.Log.Path = "./logs"
	}
	if rc.Log.Filename == "" {
		rc.Log.Filename = "replicator"
	}
	if rc.Log.Level == "" {
		rc.Log.Level = "Trace"
	}
	if rc.Log.Service == "" {
		rc.Log.Service = "replicator"
	}
	if rc.Log.Format == "" {
		rc.Log.Format = "json"
	}

	if rc.Etcd.Addr == "" {
		return nil, fmt.Errorf("unspecified etcd addr: %+v", rc.Etcd)
	}
	if rc.Etcd.Root == "" {
		rc.Etcd.Root = "replicator"
	}

	rc.Storage.DB = "replicator"
	if err := validateMysqlConfig(&rc.Storage); err != nil {
		return nil, err
	}

	if rc.BmIni.Addr == "" {
		return nil, fmt.Errorf("unspecified bm addr: %+v", rc.BmIni)
	}

	if rc.BmIni.Timeout == "" {
		rc.BmIni.Timeout = "5s"
	}

	duration, err := time.ParseDuration(rc.BmIni.Timeout)
	if err != nil {
		return nil, err
	}

	rc.BM = &bm.ServerConfig{
		Addr:    rc.BmIni.Addr,
		Timeout: xtime.Duration(duration),
	}

	return &rc, nil
}

func validateMysqlConfig(cfg *Mysql) error {
	if cfg.Host == "" || cfg.Port == 0 || cfg.Username == "" || cfg.DB == "" {
		return fmt.Errorf("invalid mysql config: %+v", cfg)
	}

	return nil
}

func MysqlConfigToConnParams(cfg *Mysql) *mysql.ConnParams {
	return &mysql.ConnParams{
		Host:             cfg.Host,
		Port:             cfg.Port,
		Uname:            cfg.Username,
		Pass:             cfg.Password,
		DbName:           cfg.DB,
		Charset:          cfg.Charset,
		ConnectTimeoutMs: cfg.ConnectTimeoutMs,
	}
}
