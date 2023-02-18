package pushc

import (
  "context"
  "encoding/binary"
	"errors"
	"fmt"
  "github.com/xpwu/go-log/log"
  "github.com/xpwu/go-stream/push/core"
  "github.com/xpwu/go-streamclient/transport"
  "net"
  "sync"
  "time"
)

type Client struct {
  transport *transport.Transport
}

var (
  clients = sync.Map{}
)

func New(ctx context.Context, addr string) *Client {
	// 连接的ctx 与 请求的ctx 不是同一个
	ctx, logger := log.WithCtx(ctx)
	logger.PushPrefix(fmt.Sprintf("push client(connect to %s).", addr))

	return &Client{
		transport: transport.New(ctx, &connector{}, addr),
	}
}

func SendTo(ctx context.Context, addr string, data []byte, token string, subP byte,
  timeout time.Duration) (res *Response, err error) {

	c, ok := clients.Load(addr)
	if !ok {
		// 链接的ctx 与 请求的ctx 不是同一个
		c, _ = clients.LoadOrStore(addr, New(context.TODO(), addr))
	}
	client := c.(*Client)
	return client.Send(ctx, data, token, subP, timeout)
}

/**
	Request:
   token | subprotocol | len | <data>
     sizeof(token) = 32 . hex
     sizeof(subprotocol) = 1.
     sizeof(len) = 4. len = sizof(data) net order
     data: subprotocol Request data

  Response:
   state
    sizeof(state) = 1.
    state = 0: success;         1: hostname error;
            2: token not exist; 3: server intelnal error;
            4: timeout
*/

func request(data []byte, token string, subP byte) net.Buffers {
  buffer := make([][]byte, 3)
  buffer[0] = []byte(token)
  buffer[1] = make([]byte, 5)
  buffer[1][0] = subP
  binary.BigEndian.PutUint32(buffer[2][1:], uint32(len(data)))
  buffer[2] = data

  return buffer
}

type Response struct {
  State core.State
}

func (c *Client) Send(ctx context.Context, data []byte, token string, subP byte,
  timeout time.Duration) (res *Response, err error) {

  ctx, logger := log.WithCtx(ctx)
  logger.PushPrefix(fmt.Sprintf("push token=%s.", token))

  if len(token) != core.TokenLen {
		str := fmt.Sprintf("token len must be %d", core.TokenLen)
    logger.Error(str)
    return nil, errors.New(str)
  }

  resD, err := c.transport.Send(ctx, request(data, token, subP), timeout)
  if len(resD) != 1 {
    return nil, errors.New("transport.Send(): return data err, len must be 1")
  }
  if err != nil {
    return nil, err
  }

  return &Response{State: core.State(resD[0])}, nil
}
