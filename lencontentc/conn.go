package lencontenc

import (
  "context"
  "encoding/binary"
  "fmt"
  "github.com/xpwu/go-log/log"
  "github.com/xpwu/go-streamclient/transport"
  "github.com/xpwu/go-xnet/connid"
  "github.com/xpwu/go-xnet/xtcp"
  "io"
  "math/rand"
  "net"
  "time"
)

type conn struct {
  *xtcp.Conn
  delegate transport.ConnDelegate

  concurrent int
  maxBytes   uint32

  heartbeat    time.Duration
  frameTimeout time.Duration

  heartbeatTimer *time.Timer
}

func newConn(c *xtcp.Conn) *conn {

  ret := &conn{
    Conn:      c,
    delegate:  nil,
    maxBytes:  4 * 1024 * 1024, // 4M
    heartbeat: 4 * time.Minute,
  }
  ret.heartbeatTimer = time.AfterFunc(ret.heartbeat, func() {
    defer func() {
      if r := recover(); r != nil {
        // 自己的定时任务关闭自己
        ret.heartbeatTimer.Stop()
        _ = ret.Close()
        ret.heartbeatTimer = nil
      }
    }()

    must(ret.SetWriteDeadline(time.Now().Add(5 * time.Second)))
    // underlying write
    _,err := ret.Conn.Write([]byte{0, 0, 0, 0})
    must(err)

    ret.heartbeatTimer.Reset(ret.heartbeat)
  })

  return ret
}

func (c *conn) MaxConcurrent() int {
  return c.concurrent
}

/**
  data protocol:
    1) length | content
      length: 4 bytes, net order; length=sizeof(content)+4; length=0 => heartbeat
*/

func (c *conn) WriteBuffers(buffers net.Buffers) (n int, err error) {

  c.heartbeatTimer.Reset(c.heartbeat)

  l := 0
  for _, d := range buffers {
    l += len(d)
  }
  if uint32(l) > c.maxBytes {
    return 0, fmt.Errorf("the data of this frame is too large, must be less than %d Bytes", c.maxBytes)
  }

  buf := make(net.Buffers, 1, len(buffers)+1)
  binary.BigEndian.PutUint32(buf[0], uint32(l+4))
  buf = append(buf, buffers...)

  return c.Conn.WriteBuffers(buf)
}

func (c *conn) SetDelegate(delegate transport.ConnDelegate) {
  c.delegate = delegate
}

func must(err error) {
  if err != nil {
    panic(err)
  }
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
  defer func() {
    if r := recover(); r != nil {
      err = r.(error)
    }
  }()

  m := make([]byte, 16)
  must(c.SetDeadline(time.Now().Add(5 * time.Second)))
  _, err = io.ReadFull(c, m)
  must(err)

  if len(m) != 16 {
    return 0, fmt.Errorf("len(handshake) should be 16, actual %d", len(m))
  }
  c.heartbeat = time.Duration(binary.BigEndian.Uint16(m[0:])) * time.Second
  m = m[2:]
  c.frameTimeout = time.Duration(m[0]) * time.Second
  m = m[1:]
  c.concurrent = int(m[0])
  m = m[1:]
  c.maxBytes = binary.BigEndian.Uint32(m[0:])
  m = m[4:]

  c.heartbeatTimer.Reset(c.heartbeat)

  return connid.Id(binary.BigEndian.Uint64(m)), nil
}

/*
lencontent protocol:

 1, handshake protocol:

                   client ------------------ server
                      |                          |
                      |                          |
                   ABCDEF (A^...^F = 0xff) --->  check(A^...^F == 0xff) --- N--> over
                    (A is version)
                      |                          |
                      |                          |Y
                      |                          |
 version 1:   set client heartbeat  <----- HeartBeat_s (2 bytes, net order)
 version 2:       set config     <-----  HeartBeat_s | FrameTimeout_s | MaxConcurrent | MaxBytes | connect id
                                          HeartBeat_s: 2 bytes, net order
                                          FrameTimeout_s: 1 byte
                                          MaxConcurrent: 1 byte
                                          MaxBytes: 4 bytes, net order
                                          connect id: 8 bytes, net order
                      |                          |
                      |                          |
                      |                          |
                      data      <-------->       data


   2, data protocol:
     1) length | content
       length: 4 bytes, net order; length=sizeof(content)+4; length=0 => heartbeat
*/

func (c *conn) read() {
  _, logger := log.WithCtx(c.Context())
  go func() {
    length := make([]byte, 4)
    defer func() {
      if r := recover(); r != nil {
        logger.Error(r)
        c.delegate.OnClosed(c.Id())
        _ = c.Close()
      }
    }()
    for {
      logger.Debug("read ...")
      must(c.SetReadDeadline(time.Now().Add(2 * c.heartbeat)))
      _, err := io.ReadFull(c.Conn, length)
      must(err)

      l := binary.BigEndian.Uint32(length)
      // heartbeat
      if l == 0 {
        log.Debug("receive heartbeat")
        continue
      }
      if l <= 4 {
        panic(fmt.Sprintf("length must be more than 4, but is %d", l))
      }
      if l > c.maxBytes {
        panic(fmt.Sprintf("length must be less than %d, but is %d", c.maxBytes, l))
      }

      must(c.SetReadDeadline(time.Now().Add(c.frameTimeout)))
      data := make([]byte, l)
      _, err = io.ReadFull(c.Conn, data)
      must(err)

      c.delegate.OnReceived(data)
    }
  }()
}

func handshake() []byte {
  hs := make([]byte, 6)
  // version: 2
  hs[0] = 2
  rand.Seed(time.Now().UnixNano())
  hs[1] = byte(rand.Intn(256))
  hs[2] = byte(rand.Intn(256))
  hs[3] = byte(rand.Intn(256))
  hs[4] = byte(rand.Intn(256))
  hs[5] = 0xff
  for i := 0; i < 5; i++ {
    hs[5] ^= hs[i]
  }

  return hs
}

type connector struct {
}

func (c *connector) Connect(ctx context.Context, addr string) (ret transport.Conn, err error) {

  ctx, logger := log.WithCtx(ctx)
  logger.PushPrefix(fmt.Sprintf("connect to %s, ", addr))

  xconn, err := xtcp.Dial(ctx, "tcp", addr)
  if err != nil {
    logger.Error(err)
    return nil, err
  }
  logger.PopPrefix()

  connect := newConn(xtcp.NewConn(ctx, xconn))

  logger.Debug(fmt.Sprintf("connected(id:%s), ", connect.Conn.Id()))

  logger.Debug("write handshake to ", addr)
  _, err = connect.WriteBuffers(append(net.Buffers{}, handshake()))
  if err != nil {
    logger.Error(err)
    return nil, err
  }
  logger.Debug("read handshake from ", addr)
  cid, err := connect.readHandshake()
  if err != nil {
    logger.Error(err)
    return nil, err
  }

  logger.Debug("end. The cid of the sever is " + cid.String())

  connect.read()

  return connect, nil
}
