package middleware

import (
	"encoding/base64"
	"strings"

	bm "github.com/go-kratos/kratos/pkg/net/http/blademaster"
)

type BasicAuth struct{}

const (
	// Authorization: Basic bWFzdGVyOmRiYWdlbnQzLjE0MTU5MjY=
	username = "replicator"
	password = "dtsfor3.1415926"
)

func (a *BasicAuth) ServeHTTP(ctx *bm.Context) {
	if !checkAuth(ctx) {
		ctx.Writer.Header().Set("WWW-Authenticate", `Basic realm="MY REALM"`)
		ctx.AbortWithStatus(401)
		return
	}
	ctx.Next()
}

func checkAuth(ctx *bm.Context) bool {
	s := strings.SplitN(ctx.Request.Header.Get("Authorization"), " ", 2)
	if len(s) != 2 {
		return false
	}

	b, err := base64.StdEncoding.DecodeString(s[1])
	if err != nil {
		return false
	}

	pair := strings.SplitN(string(b), ":", 2)
	if len(pair) != 2 {
		return false
	}
	return pair[0] == username && pair[1] == password
}
