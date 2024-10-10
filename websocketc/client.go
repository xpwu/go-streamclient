package websocketc

import (
	"context"
	"github.com/xpwu/go-streamclient/fakehttpc"
	"strings"
)

type Client struct {
	fakehttpc.Client
}

func New(ctx context.Context, addr string) *Client {
	if !strings.HasPrefix(addr, "ws://") && !strings.HasPrefix(addr, "wss://") {
		addr = "ws://" + addr
	}

	ret := &Client{
		Client: *fakehttpc.NewClient(ctx, addr, &connector{}),
	}

	return ret
}
