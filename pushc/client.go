package pushc

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/xpwu/go-log/log"
	"github.com/xpwu/go-stream/push/core"
	"github.com/xpwu/go-stream/push/protocol"
	"github.com/xpwu/go-streamclient/transport"
	"github.com/xpwu/go-xnet/xtcp"
	"sync"
	"time"
)

type conn struct {
	*xtcp.Conn
	delegate transport.ConnDelegate
}

func (c *conn) SetDelegate(delegate transport.ConnDelegate) {
	c.delegate = delegate
}

type connector struct {
}

func (c *connector) Connect(ctx context.Context, addr string) (conn transport.Conn, err error) {

}

type client struct {
	transport *transport.Transport
}

var (
	clients = sync.Map{}
)

func sendTo(ctx context.Context, addr string, data []byte, token string, subP byte,
	timeout time.Duration) (res *protocol.Response, err error) {

	// 链接的ctx 与 请求的ctx 不是同一个
	cctx, logger := log.WithCtx(context.TODO())
	logger.PushPrefix(fmt.Sprintf("push client(connect to %s).", addr))

	actual,_ := clients.LoadOrStore(addr, &client{
		transport: transport.New(cctx, &connector{}, addr),
	})
	c := actual.(*client)

	return c.send(ctx, data, token, subP, timeout)
}

/**
	Request:
   token | subprotocol | len | <data>
     sizeof(token) = 32 . hex
     sizeof(subprotocol) = 1.
     sizeof(len) = 4. len = sizof(data) net order
     data: subprotocol Request data

  Response:
   state | len | <data>
     sizeof(state) = 1.
               state = 0: success; 1: hostname error
                ; 2: token not exist; 3: server intelnal error
     sizeof(len) = 4. len = sizeof(data) net order
     data: subprotocol Response data
*/

func (c *client) send(ctx context.Context, data []byte, token string, subP byte,
	timeout time.Duration) (res *protocol.Response, err error) {

	if len(token) != core.TokenLen {
		return nil, fmt.Errorf("token len must be %d", core.TokenLen)
	}

	buffer := make([][]byte, 3)
	buffer[0] = []byte(token)
	buffer[1] = make([]byte, 5)
	buffer[1][0] = subP
	binary.BigEndian.PutUint32(buffer[2][1:], uint32(len(data)))
	buffer[2] = data

	resD, err := c.transport.Send(ctx, buffer, timeout)
	// todo parse resD
	// todo PushPrefix(fmt.Sprintf("push to conn(token=%s). ", token))
}

func (c *client) read(conn *xtcp.Conn, connClosed chan struct{}) {
	_, logger := log.WithCtx(c.ctx)
	logger.PushPrefix(fmt.Sprintf("read response from conn(id=%s),", conn.Id().String()))
	go func() {
		for {
			logger.Debug("read... ")
			r, err := protocol.NewResByConn(conn, time.Time{})
			if err != nil {
				logger.Error(err)
				logger.Info("close connect " + conn.Id().String())
				c.close(conn, connClosed)
				break
			}

			rc, ok := c.popChan(r.R.GetSequence())
			if !ok {
				logger.Warning(fmt.Sprintf("not find request of reqid(%d)", r.R.GetSequence()))
				continue
			}

			rc <- r
			close(rc)
		}
	}()
}
