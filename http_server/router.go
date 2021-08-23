package http_server

import (
	"git.100tal.com/wangxiao_jichujiagou_common/dts/http_server/middleware"
)

func (s *HttpServer) initRouter() {
	// s.NoMethod(middleware.NoMethodHandler())
	// s.NoRoute(middleware.NoRouteHandler())

	// s.Use(middleware.UserAuthMiddleware(
	//	jwtAuth,
	//	middleware.AllowMethodAndPathPrefixSkipper(
	//		//添加token认证例外
	//		middleware.JoinRouter("GET", "/api/v1/login"),
	//	),
	// ))

	/*---------------------------dispatcher-----------------------------------------------*/
	{
		task := s.Group("v1/dispatch")
		task.Use(new(middleware.BasicAuth))
		task.POST("submit", s.submitTask)
		task.POST("alter/:id", s.dispatch)
		task.PUT("alter/:id", s.dispatch)
		task.PUT("recover/:id", s.dispatch)
		task.PUT("pause/:id", s.dispatch)
		task.DELETE("delete/:id", s.dispatch)
	}

	/*---------------------------task-----------------------------------------------*/
	{
		task := s.Group("v1/task")
		task.Use(new(middleware.BasicAuth))
		task.POST("submit", s.runTask)
		task.GET("list", s.listTasks)
		task.GET("detail/:id", s.detailTask)
		task.PUT("recover/:id", s.recoverTask)
		task.PUT("alter/:id", s.alterTask)
		task.POST("alter/:id", s.alterTask)
		task.PUT("pause/:id", s.pauseTask)
		task.DELETE("delete/:id", s.deleteTask)
	}
}
