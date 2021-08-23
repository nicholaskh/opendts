package http_server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"strconv"
	"strings"

	bm "github.com/go-kratos/kratos/pkg/net/http/blademaster"

	"git.100tal.com/wangxiao_jichujiagou_common/dts/log"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/http_server/vo"
)

func (s *HttpServer) submitTask(ctx *bm.Context) {
	targetAddr, err := s.getLowestLoadInstance(ctx)
	if err != nil {
		log.Warn("get lowest load instance error: %v", err)
		ctx.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	res, err, status := requestRealServer(ctx, targetAddr)
	if err != nil {
		log.Warn("%v", err)
		ctx.AbortWithStatus(status)
		return
	}

	ctx.JSON(vo.SubmitTaskRes{
		Addr: targetAddr,
		Id:   res.Data.Id,
	}, nil)
	return
}

func (s *HttpServer) dispatch(ctx *bm.Context) {
	idStr, _ := ctx.Params.Get("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		log.Warn("invalid id: %s", idStr)
		ctx.AbortWithStatus(http.StatusBadRequest)
		return
	}
	serviceAddr, err := s.getServiceAddrById(ctx, id)
	if err != nil {
		log.Warn("%v", err)
		ctx.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	if serviceAddr == "" {
		ctx.AbortWithStatus(http.StatusNotFound)
		return
	}

	_, err, status := requestRealServer(ctx, serviceAddr)
	if err != nil {
		log.Warn("%v", err)
		ctx.AbortWithStatus(status)
		return
	}

	ctx.JSON(vo.DispatchTaskRes{}, nil)
	return
}

func requestRealServer(ctx *bm.Context, targetAddr string) (*vo.Response, error, int) {
	body, err := ioutil.ReadAll(ctx.Request.Body)
	if err != nil {
		log.Warn("get request body error: %v", err)
		return nil, err, http.StatusBadRequest
	}

	newPath := strings.Replace(ctx.Request.URL.Path, "/dispatch/", "/task/", 1)
	url := fmt.Sprintf("http://%s%s", targetAddr, newPath)
	newReq, err := http.NewRequest(ctx.Request.Method, url, bytes.NewBuffer(body))
	if err != nil {
		log.Warn("new run task request error: %v", err)
		return nil, err, http.StatusInternalServerError
	}
	newReq.Header.Set("Authorization", ctx.Request.Header.Get("Authorization"))
	newReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(newReq)
	if err != nil {
		log.Warn("send run task request error: %v", err)
		return nil, err, http.StatusInternalServerError
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("request real server error: code %v", resp.StatusCode)
		log.Warn("%v", err)
		ctx.AbortWithStatus(resp.StatusCode)
		return nil, err, http.StatusBadRequest
	}
	body, _ = ioutil.ReadAll(resp.Body)
	var res vo.Response
	err = json.Unmarshal(body, &res)
	if err != nil {
		log.Warn("get run task response error: %v", err)
		return nil, err, http.StatusInternalServerError
	}
	if res.Code != 0 {
		err = fmt.Errorf("get run task response code error: %d", res.Code)
		log.Warn("%v", err)
		return nil, err, http.StatusInternalServerError
	}

	return &res, nil, 0
}

func (s *HttpServer) getLowestLoadInstance(ctx *bm.Context) (string, error) {
	instances, err := s.r.ListInstances()
	if err != nil {
		return "", err
	}
	m := make(map[string]bool, len(instances))
	for _, instance := range instances {
		parts := strings.Split(instance, "/")
		m[parts[len(parts)-1]] = false
	}
	query := fmt.Sprintf("select service_addr,count(*) cnt from replicator.replication group by service_addr")
	result, err := s.dbClient.ExecuteWithRetry(ctx, query)
	if err != nil {
		log.Warn("get tasks cnt from db error: %s", err.Error())
		return "", err
	}
	var curAddr, addr string
	var cnt int64
	min := int64(math.MaxInt64)
	for _, row := range result.Rows {
		curAddr = row[0].ToString()
		cnt, err = row[1].ToInt64()
		if err != nil {
			return "", err
		}
		if _, ok := m[curAddr]; ok {
			if cnt < min {
				min = cnt
				addr = curAddr
			}
			m[curAddr] = true
		}
	}

	for instance, exists := range m {
		if !exists {
			return instance, nil
		}
	}

	return addr, nil
}

func (s *HttpServer) getServiceAddrById(ctx *bm.Context, id int) (string, error) {
	result, err := s.dbClient.GetTask(ctx, id)
	if err != nil {
		log.Warn("get task from db error: %s", err.Error())
		return "", err
	}
	if len(result.Rows) == 0 {
		log.Warn("task not found for id: %d", id)
		return "", nil
	}
	task, err := rowToTask(result.Rows[0])
	return task.ServiceAddr, err
}
