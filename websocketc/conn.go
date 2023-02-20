package websocketc

import (
  "context"
  "encoding/binary"
  "fmt"
  "github.com/gorilla/websocket"
  "github.com/xpwu/go-log/log"
  "github.com/xpwu/go-streamclient/transport"
  "github.com/xpwu/go-xnet/connid"
  "github.com/xpwu/go-xnet/xtcp"
  "net"
  "sync"
  "time"
)

type conn struct {
  c        *websocket.Conn
  delegate transport.ConnDelegate
  id       connid.Id
  mu       chan struct{}
  ctx      context.Context
  once     sync.Once

  concurrent int
  maxBytes   uint32
}

func newConn(ctx context.Context, c *websocket.Conn) *conn {
  id := connid.Id(0)
  underConn, ok := c.UnderlyingConn().(*xtcp.Conn)
  if ok {
    id = underConn.Id()
  } else {
    id = connid.New()
  }

  return &conn{
    c:        c,
    delegate: nil,
    id:       id,
    ctx:      ctx,
    maxBytes: 4 * 1024 * 1024, // 4M
  }
}

func (c *conn) Close() (err error) {
  c.once.Do(func() {
    _, logger := log.WithCtx(c.ctx)
    logger.Info("close connection")
    err = c.c.Close()
  })
  return
}

func (c *conn) MaxConcurrent() int  {
  return c.concurrent
}

func (c *conn) WriteBuffers(buffers net.Buffers) (n int, err error) {
  // 只能一个goroutines 访问
  c.mu <- struct{}{}
  defer func() {
    <-c.mu
  }()

  err = c.c.SetWriteDeadline(time.Now().Add(5 * time.Second))
  if err != nil {
    return -1, err
  }

  writer, err := c.c.NextWriter(websocket.BinaryMessage)
  if err != nil {
    return -1, err
  }

  l := 0
  for _, d := range buffers {
    l += len(d)
  }
  if uint32(l) > c.maxBytes {
    return 0, fmt.Errorf("the data of this frame is too large, must be less than %d Bytes", c.maxBytes)
  }

  l = 0
  for _, d := range buffers {
    l += len(d)
    if _, err = writer.Write(d); err != nil {
      return l, err
    }
  }

  return l, writer.Close()
}

func (c *conn) SetDelegate(delegate transport.ConnDelegate) {
  c.delegate = delegate
}

func (c *conn) Id() connid.Id {
  return c.id
}

/*
HeartBeat_s | FrameTimeout_s | MaxConcurrent | MaxBytes | connect id
   HeartBeat_s: 2 bytes, net order
   FrameTimeout_s: 1 byte  ===0
   MaxConcurrent: 1 byte
   MaxBytes: 4 bytes, net order
   connect id: 8 bytes, net order
*/
func (c *conn) readHandshake() (peerConnectionID connid.Id, err error) {
  // read handshake
  _, m, err := c.c.ReadMessage()
  if err != nil {
    return 0, err
  }
  if len(m) != 16 {
    return 0, fmt.Errorf("len(handshake) should be 16, actual %d", len(m))
  }
  m = m[3:] // ignore HeartBeat_s  FrameTimeout_s
  c.concurrent = int(m[0])
  m = m[1:]
  c.maxBytes = binary.BigEndian.Uint32(m[0:])
  m = m[4:]
  return connid.Id(binary.BigEndian.Uint64(m)), nil
}

func (c *conn) read() {
  _, logger := log.WithCtx(c.ctx)
  go func() {
    for {
      logger.Debug("read ...")
      _, m, err := c.c.ReadMessage()
      if err != nil {
        logger.Error(err)
        c.delegate.OnClosed(c.id)
        _ = c.Close()
        return
      }

      c.delegate.OnReceived(m)
    }
  }()
}

type connector struct {
}

func (c *connector) Connect(ctx context.Context, addr string) (ret transport.Conn, err error) {

  ctx, logger := log.WithCtx(ctx)

  logger.PushPrefix(fmt.Sprintf("connect to %s, ", addr))
  defer logger.PopPrefix()

  logger.Debug("start")
  wConn, _, err := websocket.DefaultDialer.DialContext(ctx, addr, nil)
  if err != nil {
    logger.Error(err)
    return nil, err
  }

  connect := newConn(ctx, wConn)
  logger.Debug("read handshake")
  cid, err := connect.readHandshake()
  if err != nil {
    logger.Error(err)
    return nil, err
  }

  logger.Debug("end. The cid of the sever is " + cid.String())

  connect.read()

  return connect, nil
}
