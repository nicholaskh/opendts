package http_server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	bm "github.com/go-kratos/kratos/pkg/net/http/blademaster"
	"github.com/go-kratos/kratos/pkg/net/ip"

	"github.com/nicholaskh/opendts"
	"github.com/nicholaskh/opendts/http_server/vo"
	"github.com/nicholaskh/opendts/log"
	"github.com/nicholaskh/opendts/mysql"
)

const (
	DefaultRate     = 30000
	DefaultCapacity = 60000

	InspectTasksInterval = 60 * time.Second
	TaskDelayTimeout     = 15 * time.Minute

	TimeFormat = "2006-01-02 15:04:05"
)

func (s *HttpServer) runTask(ctx *bm.Context) {
	req := new(vo.RunTaskReq)
	err := ctx.Bind(req)
	if err != nil {
		log.Warn("bind submitTask req error: %v", err)
		ctx.AbortWithStatus(http.StatusUnprocessableEntity)
		return
	}

	if req.Task.Source == nil {
		log.Warn("req source is nil")
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}

	if req.Task.Dest == nil && req.Task.Kafka == nil {
		log.Warn("no destination specified")
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}

	if req.Task.Source.Host == "" ||
		req.Task.Source.Username == "" {
		log.Warn("req source fields is empty")
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}

	if req.Task.Source.Port == 0 {
		req.Task.Source.Port = 3306
	}

	if req.Task.Dest != nil && req.Task.Dest.Port == 0 {
		req.Task.Dest.Port = 3306
	}

	source := mysqlToParams(req.Task.Source)
	var dest *mysql.ConnParams
	if req.Task.Dest != nil {
		dest = mysqlToParams(req.Task.Dest)
	}
	filter, err := filtersToProto(req.Task.Filters)
	if err != nil {
		log.Warn("filters to proto error: %v", err)
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}
	var kafkaParams *replicator.KafkaParams
	if req.Task.Kafka != nil {
		kafkaParams, err = kafkaParamsToInternal(req.Task.Kafka)
		if err != nil {
			ctx.AbortWithStatus(http.StatusBadRequest)
		}
	}

	ip := ip.InternalIP()
	addr := fmt.Sprintf("%s:%d", ip, s.port)
	if req.Task.RateLimit == nil {
		req.Task.RateLimit = &vo.RateLimit{
			Rate:     DefaultRate,
			Capacity: DefaultCapacity,
		}
	}

	r, err := replicator.NewReplicator(source, dest, kafkaParams, filter, req.Task.RateLimit, s.stats, req.Task.IsIncr, addr)
	if err != nil {
		log.Warn("new replicator error: %v", err)
		ctx.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Warn("%v", err)
				debug.PrintStack()
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())

		s.mu.Lock()
		s.taskCancel[r.Id] = cancel
		s.taskReplicator[r.Id] = r
		s.mu.Unlock()
		err = r.Replicate(ctx)
		if err != nil {
			log.Warn("start replication error: %v", err)
			return
		}
	}()

	ctx.JSON(vo.RunTaskRes{r.Id}, nil)
	return
}

func (s *HttpServer) listTasks(ctx *bm.Context) {
	perPageStr := ctx.Request.Form.Get("per_page")
	pageStr := ctx.Request.Form.Get("page")

	perPage, err := strconv.Atoi(perPageStr)
	if perPageStr != "" && err != nil {
		log.Warn("invalid per_page: %s", perPageStr)
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}
	page, err := strconv.Atoi(pageStr)
	if pageStr != "" && err != nil {
		log.Warn("invalid page: %s", pageStr)
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}

	if page < 1 {
		page = 1
	}
	if perPage < 1 {
		perPage = 10
	}

	result, err := s.dbClient.ListTasks(ctx, perPage, page)
	if err != nil {
		log.Warn("list tasks from db error: %s", err.Error())
		ctx.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	tasks, err := rowsToTasks(result.Rows)
	if err != nil {
		log.Warn("list tasks rows to tasks error: %s", err.Error())
		ctx.AbortWithStatus(http.StatusInternalServerError)
	}
	ctx.JSON(vo.ListTasksRes{tasks}, nil)
	return
}

func (s *HttpServer) detailTask(ctx *bm.Context) {
	idStr, _ := ctx.Params.Get("id")

	id, err := strconv.Atoi(idStr)
	if err != nil {
		log.Warn("invalid id: %s", idStr)
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}
	result, err := s.dbClient.GetTask(ctx, id)
	if err != nil {
		log.Warn("get task from db error: %s", err.Error())
		ctx.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	if len(result.Rows) == 0 {
		log.Warn("task not found for id: %d", id)
		ctx.AbortWithStatus(http.StatusNotFound)
		return
	}
	task, err := rowToTask(result.Rows[0])
	if err != nil {
		log.Warn("detail task row to task error: %s", err.Error())
		ctx.AbortWithStatus(http.StatusInternalServerError)
	}
	ctx.JSON(vo.DetailTaskRes{task}, nil)
	return
}

func (s *HttpServer) recoverTask(ctx *bm.Context) {
	idStr, _ := ctx.Params.Get("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		log.Warn("invalid id: %s", idStr)
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}

	addr := fmt.Sprintf("%s:%d", ip.InternalIP(), s.port)
	isLocal, err := s.checkLocal(ctx, id, addr)
	if err != nil {
		log.Warn("%v", err)
		ctx.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	if !isLocal {
		log.Warn("task[%d] not local", id)
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	if task, ok := s.taskReplicator[id]; ok && task.State == replicator.BlpRunning {
		log.Warn("task %d already running", id)
		ctx.AbortWithStatus(http.StatusBadRequest)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Warn("%v", err)
			}
		}()
		r, err := replicator.LoadFromDB(id, s.stats, addr)
		if err != nil {
			log.Warn("new replicator error: %v", err)
			return
		}
		ctx, cancel := context.WithCancel(context.Background())

		s.mu.Lock()
		s.taskCancel[r.Id] = cancel
		s.taskReplicator[r.Id] = r
		s.mu.Unlock()
		err = r.Replicate(ctx)
		if err != nil {
			log.Warn("recover replication error: %v", err)
			return
		}
	}()

	ctx.JSON(vo.RecoverTaskRes{}, nil)
	return
}

func (s *HttpServer) alterTask(ctx *bm.Context) {
	idStr, _ := ctx.Params.Get("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		log.Warn("invalid id: %s", idStr)
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}
	req := new(vo.AlterTaskReq)
	err = ctx.Bind(req)
	if err != nil {
		log.Warn("bind alterTask req error: %v", err)
		ctx.AbortWithStatus(http.StatusUnprocessableEntity)
		return
	}

	filter, err := filtersToProto(req.Task.Filters)
	if err != nil {
		log.Warn("filters to proto error: %v", err)
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}
	var kafkaParams *replicator.KafkaParams
	if req.Task.Kafka != nil {
		kafkaParams, err = kafkaParamsToInternal(req.Task.Kafka)
		if err != nil {
			ctx.AbortWithStatus(http.StatusBadRequest)
		}
	}

	if req.Task.RateLimit == nil {
		req.Task.RateLimit = &vo.RateLimit{
			Rate:     DefaultRate,
			Capacity: DefaultCapacity,
		}
	}

	s.mu.Lock()
	if cancel, ok := s.taskCancel[id]; ok {
		cancel()
		<-s.taskReplicator[id].Done()
	}
	s.mu.Unlock()

	addr := fmt.Sprintf("%s:%d", ip.InternalIP(), s.port)
	isLocal, err := s.checkLocal(ctx, id, addr)
	if err != nil {
		log.Warn("%v", err)
		ctx.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	if !isLocal {
		log.Warn("task[%d] not local", id)
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}

	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Warn("%v", err)
			}
		}()

		sql := "update replicator.replication set "
		prefix := ""

		if req.Task.Source != nil {
			source := mysqlToParams(req.Task.Source)
			sourceConnectParams, err := json.Marshal(source)
			if err != nil {
				log.Warn("%v", err)
				return
			}
			sql += fmt.Sprintf("source_addr='%s:%d',source_connect_params='%s'", source.Host, source.Port, string(sourceConnectParams))
			prefix = ","
		}

		if filter != nil {
			bs, err := json.Marshal(filter)
			if err != nil {
				log.Warn("%v", err)
				return
			}
			sql += prefix + "filter='" + string(bs) + "'"
			prefix = ","
		}
		if kafkaParams.Addr != "" {
			bs, err := json.Marshal(kafkaParams)
			if err != nil {
				log.Warn("%v", err)
				return
			}
			sql += prefix + "kafka_params='" + string(bs) + "'"
			prefix = ","
		}
		if req.Task.RateLimit.Rate != 0 {
			bs, err := json.Marshal(req.Task.RateLimit)
			if err != nil {
				log.Warn("%v", err)
				return
			}
			sql += prefix + "rate_limit='" + string(bs) + "'"
			prefix = ","
		}
		sql += " where id=" + strconv.Itoa(id)

		query := "set sql_mode='NO_BACKSLASH_ESCAPES'"
		if _, err = s.dbClient.ExecuteWithRetry(ctx, query); err != nil {
			log.Warn("could not set sql_mode to NO_BACKSLASH_ESCAPES: %s: %v", query, err)
			return
		}

		_, err = s.dbClient.ExecuteWithRetry(ctx, sql)
		if err != nil {
			log.Warn("%v", err)
			return
		}

		r, err := replicator.LoadFromDB(id, s.stats, addr)
		if err != nil {
			log.Warn("new replicator error: %v", err)
			return
		}

		ctx, cancel := context.WithCancel(context.Background())

		s.mu.Lock()
		s.taskCancel[r.Id] = cancel
		s.taskReplicator[r.Id] = r
		s.mu.Unlock()
		err = r.Replicate(ctx)
		if err != nil {
			log.Warn("alter replication error: %v", err)
			return
		}
	}()

	ctx.JSON(vo.AlterTaskRes{}, nil)
	return
}

func (s *HttpServer) pauseTask(ctx *bm.Context) {
	idStr, _ := ctx.Params.Get("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		log.Warn("invalid id: %s", idStr)
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}

	addr := fmt.Sprintf("%s:%d", ip.InternalIP(), s.port)
	isLocal, err := s.checkLocal(ctx, id, addr)
	if err != nil {
		log.Warn("%v", err)
		ctx.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	if !isLocal {
		log.Warn("task[%d] not local", id)
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	delete(s.taskReplicator, id)
	if cancel, ok := s.taskCancel[id]; ok {
		cancel()
	}
	s.mu.Unlock()

	ctx.JSON(vo.PauseTaskRes{}, nil)
	return
}

func (s *HttpServer) deleteTask(ctx *bm.Context) {
	idStr, _ := ctx.Params.Get("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		log.Warn("invalid id: %s", idStr)
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}

	addr := fmt.Sprintf("%s:%d", ip.InternalIP(), s.port)
	isLocal, err := s.checkLocal(ctx, id, addr)
	if err != nil {
		log.Warn("%v", err)
		ctx.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	if !isLocal {
		log.Warn("task[%d] not local", id)
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	delete(s.taskReplicator, id)
	if cancel, ok := s.taskCancel[id]; ok {
		cancel()
	}
	s.mu.Unlock()

	err = s.dbClient.DeleteTask(ctx, id)
	if err != nil {
		log.Warn("%v", err)
		ctx.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	ctx.JSON(vo.DeleteTaskRes{}, nil)
	return
}

func (s *HttpServer) RecoverLocalTasks(addr string) error {
	result, err := s.dbClient.ListLocalTaskIds(addr)
	if err != nil {
		return err
	}

	for _, row := range result.Rows {
		tid, err := row[0].ToInt64()
		if err != nil {
			log.Warn("invalid task id: %v", err)
			return err
		}
		go func(tid int64) {
			defer func() {
				if err := recover(); err != nil {
					log.Warn("%v", err)
				}
			}()
			r, err := replicator.LoadFromDB(int(tid), s.stats, addr)
			if err != nil {
				log.Warn("load replicator error: %v", err)
				return
			}
			ctx, cancel := context.WithCancel(context.Background())

			s.mu.Lock()
			s.taskCancel[r.Id] = cancel
			s.taskReplicator[r.Id] = r
			s.mu.Unlock()
			err = r.Replicate(ctx)
			if err != nil {
				log.Warn("recover replication error: %v", err)
				return
			}
		}(tid)
	}

	return nil
}

func (s *HttpServer) TimelyInspectLocalTasks() error {
	timer := time.NewTimer(InspectTasksInterval)
	defer timer.Stop()
	localAddr := fmt.Sprintf("%s:%d", ip.InternalIP(), s.port)
	for {
		now := time.Now()
		select {
		case <-timer.C:
			result, err := s.dbClient.ListLocalTasks(context.Background(), localAddr)
			if err != nil {
				log.Warn("list local tasks from db error: %s", err.Error())
				return err
			}
			tasks, err := rowsToTasks(result.Rows)
			if err != nil {
				log.Warn("list local tasks rows to tasks error: %s", err.Error())
				return err
			}
			for _, t := range tasks {
				updatedAt, err := time.Parse(TimeFormat, t.UpdatedAt)
				if err != nil {
					log.Warn("invalid updatedAt: %s", t.UpdatedAt)
					continue
				}
				transactionTime, err := time.Parse(TimeFormat, t.TransactionTime)
				if err != nil {
					log.Warn("invalid transactionTime: %s", t.TransactionTime)
					continue
				}
				switch {
				case t.State != replicator.BlpRunning:
					log.Warn("task %d not running, current state: %s, message: %s, prepare to restart......", t.Id, t.State, t.Message)
				case now.After(updatedAt.Add(TaskDelayTimeout)):
					log.Warn("task %d not updated, now: %s, updated_at: %s, prepare to restart......", t.Id, now.Format(TimeFormat), t.UpdatedAt)
				case now.After(transactionTime.Add(TaskDelayTimeout)):
					log.Warn("task %d not update transaction_time, now: %s, transaction_time: %s, prepare to restart......", t.Id, now.Format(TimeFormat), t.TransactionTime)
				default:
					continue
				}

				s.mu.Lock()
				if cancel, ok := s.taskCancel[t.Id]; ok {
					r, ok := s.taskReplicator[t.Id]
					s.mu.Unlock()
					cancel()
					if ok {
						<-r.Done()
					}
				} else {
					s.mu.Unlock()
				}

				go func(id int) {
					defer func() {
						if err := recover(); err != nil {
							log.Warn("%v", err)
						}
					}()

					repl, err := replicator.LoadFromDB(id, s.stats, localAddr)
					if err != nil {
						log.Warn("load replicator error: %v", err)
						return
					}

					ctx, cancel := context.WithCancel(context.Background())

					s.mu.Lock()
					s.taskCancel[repl.Id] = cancel
					s.taskReplicator[repl.Id] = repl
					s.mu.Unlock()

					err = repl.Replicate(ctx)
					if err != nil {
						log.Warn("timely auto recover replication error: %v", err)
						return
					}
				}(t.Id)
			}
		}
	}
}

func (s *HttpServer) checkLocal(ctx *bm.Context, id int, addr string) (bool, error) {
	result, err := s.dbClient.GetTask(ctx, id)
	if err != nil {
		log.Warn("get task from db error: %v", err)
		return false, err
	}
	if len(result.Rows) == 0 {
		err := fmt.Errorf("invalid task id: %d", id)
		log.Warn("%v", err)
		return false, nil
	}
	task, err := rowToTask(result.Rows[0])
	if err != nil {
		log.Warn("alter task row to task error: %v", err)
		return false, err
	}
	if task.ServiceAddr != addr {
		log.Warn("task[%d] not run on this machine", id)
		return false, nil
	}

	return true, nil
}
