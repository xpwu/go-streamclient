package pushc

import (
  "context"
  "fmt"
  "github.com/xpwu/go-log/log"
  "github.com/xpwu/go-stream/push/protocol"
  "github.com/xpwu/go-streamclient/transport"
  "github.com/xpwu/go-xnet/xtcp"
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

func (c *connector) Connect(ctx context.Context, addr string) (ret transport.Conn, err error) {
  ctx, logger := log.WithCtx(ctx)
  logger.PushPrefix("connecting... ")

  xconn, err := xtcp.Dial(ctx, "tcp", addr)
  if err != nil {
    logger.Error(err)
    return nil, err
  }
  logger.PopPrefix()

  con := &conn{}
  con.Conn = xtcp.NewConn(ctx, xconn)

  logger.Debug(fmt.Sprintf("connected(id:%s), ", con.Conn.Id()))

  con.read()

  return con, nil
}

func (c *conn) read() {
  _, logger := log.WithCtx(c.Conn.Context())
  logger.PushPrefix(fmt.Sprintf("read response from conn(id=%s),", c.Conn.Id()))

  go func() {
    for {
      logger.Debug("read... ")
      res, err := protocol.NewResByConn(c.Conn, time.Time{})
      if err != nil {
        logger.Error(err)
        logger.Info("close connect " + c.Conn.Id().String())
        c.delegate.OnClosed(c.Conn.Id())
        _ = c.Conn.Close()
        break
      }

      c.delegate.OnReceived([]byte{byte(res.State)})
    }
  }()
}

