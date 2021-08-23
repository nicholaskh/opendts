package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-kratos/kratos/pkg/conf/env"
	"github.com/go-kratos/kratos/pkg/net/ip"

	"git.100tal.com/wangxiao_jichujiagou_common/dts"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/config"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/http_server"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/log"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/log/xlog"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/register"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/stats/prometheus"
)

var configPath = flag.String("config", "etc/replicator.ini", "path to replicator config file")
var skipRecoverTasks = flag.Bool("r", false, "skip recover local tasks on start, default to false")

// TODO main函数瘦身
func main() {
	env.AppID = "replicator"
	flag.Parse()
	var err error
	config.Conf, err = config.LoadConfig(*configPath)
	if err != nil {
		fmt.Printf("load config error: %v\n", err.Error())
		return
	}

	if err := initXLog(&config.Conf.Log); err != nil {
		fmt.Printf("init xlog error: %v\n", err.Error())
		return
	}
	defer log.Close()

	stats := replicator.NewStats()
	prometheus.Init(env.AppID)

	parts := strings.Split(config.Conf.BM.Addr, ":")
	if len(parts) < 2 {
		log.Fatal("invalid bm addr, no port specified: %s", config.Conf.BM.Addr)
		return
	}
	portStr := parts[len(parts)-1]
	addr := fmt.Sprintf("%s:%s", ip.InternalIP(), portStr)
	r, err := register.NewRegister(&config.Conf.Etcd)
	if err != nil {
		log.Fatal("fail to new register: %v", err)
		return
	}
	err = r.RegisterReplicator(addr)
	if err != nil {
		log.Fatal("fail to register replicator: %v", err)
		return
	}

	params := config.MysqlConfigToConnParams(&config.Conf.Storage)
	dbClient := replicator.NewDBClient(params, stats)
	err = dbClient.Connect()
	if err != nil {
		log.Fatal("init storage dbclient error: %v", err)
		return
	}

	httpSrv, err := http_server.New(config.Conf.BM, dbClient, stats, r)
	if err != nil {
		log.Fatal("fail to start http server: %v", err)
		return
	}

	if !*skipRecoverTasks {
		err = httpSrv.RecoverLocalTasks(addr)
		if err != nil {
			log.Fatal("fail to recover local tasks: %v", err)
			return
		}
	}

	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Warn("%v", err)
			}
		}()
		httpSrv.TimelyInspectLocalTasks()
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		s := <-c
		log.Trace("get a signal %s", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			if err := httpSrv.Shutdown(ctx); err != nil {
				log.Fatal("httpSrv.Shutdown error(%v)", err)
			}
			if err := r.UnregisterReplicator(); err != nil {
				log.Fatal("UnregisterReplicator error(%v)", err)
			}
			log.Trace("replicator exit")
			cancel()
			time.Sleep(time.Second)
			return
		case syscall.SIGHUP:
		default:
			return
		}
	}
}

func initXLog(logConfig *config.Log) error {
	cfg := make(map[string]string)
	cfg["path"] = logConfig.Path
	cfg["filename"] = logConfig.Filename
	cfg["level"] = logConfig.Level
	cfg["service"] = logConfig.Service
	cfg["format"] = logConfig.Format
	cfg["skip"] = "5" // 设置xlog打印方法堆栈需要跳过的层数, 5目前为调用log.Debug()等方法的方法名, 比xlog默认值多一层.

	logger, err := xlog.CreateLogManager(logConfig.Output, cfg)
	if err != nil {
		return err
	}

	log.SetGlobalLogger(logger)
	return nil
}
