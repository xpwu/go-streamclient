package transport

import (
  "context"
  "encoding/binary"
  "errors"
  "fmt"
  "github.com/xpwu/go-log/log"
  "github.com/xpwu/go-xnet/xtcp"
  "net"
  "sync"
  "time"
)

/**
protocol:
     request ---
       sequence | data
         reqid: 4 bytes, net order;
         data:       [optional]

  ---------------------------------------------------------------------
     response ---
       reqid | data
         reqid: 4 bytes, net order;
         data:       [optional]

*/

const sequenceLen = 4

type Conn interface {
  Connect(addr string)
  Close()
  SetOnReceived(func([]byte))
  WriteBuffers(buffers net.Buffers) (n int, err error)
}

type Transport struct {
  conn       Conn
  connClosed chan struct{}
  mutex      sync.RWMutex
  addr       string
  ctx        context.Context

  mq       map[uint32] chan []byte
  sequence uint32
  mqMu     sync.Mutex
}

var (
  Err = errors.New("time out")
)

func (c *Transport) Send(ctx context.Context, data []byte, timeout time.Duration) (res []byte, err error) {
  timer := time.NewTimer(timeout)
  _, logger := log.WithCtx(c.ctx)
  //logger.PushPrefix(fmt.Sprintf("push to conn(token=%s). ", token))

  res, err = c.sendOnce(ctx, data, timer)
  if err == Err {
    return
  }
  if err == nil {
    if !timer.Stop() {
      <-timer.C
    }
    return
  }

  // 非超时情况，重试一次。需要重试的原因主要是可能在发送的时候，连接断了
  logger.PushPrefix("try again, ")
  res, err = c.sendOnce(ctx, data, timer)
  if err != Err && !timer.Stop() {
    <-timer.C
  }
  return
}

func (c *Transport) sendOnce(ctx context.Context, data []byte, timer *time.Timer) (res []byte, err error) {

  _, logger := log.WithCtx(c.ctx)

  conn, connClosed, err := c.connect()
  if err != nil {
    logger.Error(err)
    return nil, err
  }
  logger.PushPrefix(fmt.Sprintf("connid=%s", conn.Id().String()))

  resCh := make(chan []byte)
  seq := c.addChan(resCh)

  buffers := make([][]byte, 2)
  buffers[0] = make([]byte, sequenceLen)
  binary.BigEndian.PutUint32(buffers[0], seq)
  buffers[1] = data

  _, err = conn.WriteBuffers(buffers)
  if err != nil {
    logger.Error(err)
    c.delChan(seq)
    c.close(conn, connClosed)
    return
  }

  select {
  case res = <-resCh:
    return

  case <-connClosed:
    err = errors.New("connection closed")
  case <-timer.C:
    err = Err
  case <-ctx.Done():
    err = ctx.Err()
  case <-c.ctx.Done():
    err = c.ctx.Err()
  }
  c.delChan(seq)
  return
}

func (c *Transport) connect() (conn Conn, connClosed chan struct{}, err error) {

 c.mutex.RLock()
 if c.conn != nil {
   ret := c.conn
   ch := c.connClosed
   c.mutex.RUnlock()
   return ret, ch, nil
 }
 c.mutex.RUnlock()

 c.mutex.Lock()
 defer c.mutex.Unlock()
 // read again
 if c.conn != nil {
   return c.conn, c.connClosed, nil
 }

 ctx, logger := log.WithCtx(c.ctx)
 logger.PushPrefix("connecting... ")

 conn, err := xtcp.Dial(ctx, "tcp", c.addr)
 if err != nil {
   logger.Error(err)
   return nil, nil, err
 }
 logger.PopPrefix()

 c.conn = xtcp.NewConn(ctx, conn)
 c.connClosed = make(chan struct{})

 logger.Debug(fmt.Sprintf("connected(id:%s), ", c.conn.Id()))

 c.read(c.conn, c.connClosed)

 return c.conn, c.connClosed, nil
}

func (c *Transport) close(old *xtcp.Conn, connClosed chan struct{}) {
 c.mutex.RLock()
 // 已有新的连接时，不做任何操作
 if c.conn == nil || old.Id() != c.conn.Id() {
   c.mutex.RUnlock()
   return
 }

 c.mutex.RUnlock()

 c.mutex.Lock()
 defer c.mutex.Unlock()

 _ = c.conn.Close()
 close(connClosed)

 c.conn = nil
}

func (c *Transport) delChan(sequence uint32) (r chan []byte, ok bool) {
  c.mqMu.Lock()
  defer c.mqMu.Unlock()

  r, ok = c.mq[sequence]
  delete(c.mq, sequence)

  return
}

func (c *Transport) addChan(ch chan []byte) uint32 {
  c.mqMu.Lock()
  defer c.mqMu.Unlock()
  c.sequence++
  c.mq[c.sequence] = ch

  return c.sequence
}

func (c *Transport) onReceived(data []byte) {
  _, logger := log.WithCtx(c.ctx)
  seq := binary.BigEndian.Uint32(data)
  rc, ok := c.delChan(seq)
  if !ok {
    logger.Warning(fmt.Sprintf("not find request of reqid(%d)", seq))
    return
  }

  rc <- data[4:]
  close(rc)
}
