package lencontenc

import (
  "context"
  "github.com/xpwu/go-streamclient/fakehttpc"
)

type Client struct {
  fakehttpc.Client
}

func New(ctx context.Context, addr string) *Client {

  ret := &Client{
    Client: *fakehttpc.NewClient(ctx, addr, &connector{}),
  }

  return ret
}

