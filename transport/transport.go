package transport

import (
  "context"
  "encoding/binary"
  "errors"
  "fmt"
  "github.com/xpwu/go-log/log"
  "github.com/xpwu/go-xnet/connid"
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

type ConnDelegate interface {
  OnReceived(data []byte)
  OnClosed(id connid.Id)
}

type Conn interface {
  Close() error
  SetDelegate(delegate ConnDelegate)
  Id() connid.Id
  WriteBuffers(buffers net.Buffers) (n int, err error)
  MaxConcurrent() int
}

type Connector interface {
  Connect(ctx context.Context, addr string) (conn Conn, err error)
}

type connSuit struct {
  // conn 必须并发安全
  conn       Conn
  connClosed chan struct{}
  concurrent chan struct{}
  once       sync.Once
}

func (c *connSuit) close() {
  c.once.Do(func() {
    _ = c.conn.Close()
    close(c.connClosed)
  })
}

func newConnSuits(conn Conn, concurrent int) *connSuit {
  return &connSuit{
    conn:       conn,
    connClosed: make(chan struct{}),
    concurrent: make(chan struct{}, concurrent),
    once:       sync.Once{},
  }
}

type Transport struct {
  connector Connector
  addr      string
  ctx       context.Context

  connMutex sync.RWMutex
  cs        *connSuit

  mq       map[uint32]chan []byte
  sequence uint32
  mqMu     sync.Mutex

  opt *option
}

type option struct {
  start      uint32
  unknownSeq func(seq uint32, data []byte)
}

type Option func(opt *option)

func StartSeq(start uint32) Option {
  return func(opt *option) {
    opt.start = start
  }
}

func UnknownSeq(f func(seq uint32, data []byte)) Option {
  return func(opt *option) {
    opt.unknownSeq = f
  }
}

func New(ctx context.Context, connector Connector, addr string, options ...Option) *Transport {
  opt := &option{
    start:      0,
    unknownSeq: nil,
  }
  for _, v := range options {
    v(opt)
  }

  return &Transport{
    connector: connector,
    addr:      addr,
    ctx:       ctx,

    connMutex: sync.RWMutex{},
    cs:        nil,

    mq:       make(map[uint32]chan []byte),
    sequence: 10,
    mqMu:     sync.Mutex{},

    opt: opt,
  }
}

var (
  TimeoutErr = errors.New("time out")
)

func (c *Transport) Send(ctx context.Context, data net.Buffers, timeout time.Duration) (res []byte, err error) {
  timer := time.NewTimer(timeout)
  _, logger := log.WithCtx(ctx)

  res, err = c.sendOnce(ctx, data, timer)
  if err == TimeoutErr {
    return
  }
  if err == nil {
    if !timer.Stop() {
      <-timer.C
    }
    return
  }

  // 非超时情况，重试一次。需要重试的原因主要是可能在发送的时候，连接断了
  logger.PushPrefix("try again,")
  res, err = c.sendOnce(ctx, data, timer)
  if err != TimeoutErr && !timer.Stop() {
    <-timer.C
  }
  return
}

func (c *Transport) Context() context.Context {
  return c.ctx
}

func (c *Transport) sendOnce(ctx context.Context, data net.Buffers, timer *time.Timer) (res []byte, err error) {

  _, tLogger := log.WithCtx(c.ctx)
  ctx, logger := log.WithCtx(ctx)

  thisCS, err := c.connect()
  if err != nil {
    tLogger.Error(err)
    return nil, err
  }

  idLStr := fmt.Sprintf("connid=%s", thisCS.conn.Id().String())
  tLogger.PushPrefix(idLStr)
  logger.PushPrefix(idLStr)

  resCh := make(chan []byte)
  seq := c.addChan(resCh)
  defer func() {
    if err != nil {
      c.delChan(seq)
    }
  }()

  select {
  case thisCS.concurrent <- struct{}{}:
    goto work

  case <-timer.C:
    err = TimeoutErr
  case <-thisCS.connClosed:
    err = errors.New("connection closed")
  case <-ctx.Done():
    err = ctx.Err()
  case <-c.ctx.Done():
    err = c.ctx.Err()
  }
  return nil, err

work:
  defer func() {
    <-thisCS.concurrent
  }()

  buffers := make([][]byte, 1, 1+len(data))
  buffers[0] = make([]byte, sequenceLen)
  binary.BigEndian.PutUint32(buffers[0], seq)
  buffers = append(buffers, data...)

  _, err = thisCS.conn.WriteBuffers(buffers)
  if err != nil {
    tLogger.Error(err)
    logger.Error(err)
    c.close(thisCS)
    return
  }

  select {
  case res = <-resCh:
    logger.Debug("succeed")
    return

  case <-thisCS.connClosed:
    err = errors.New("connection closed")
  case <-timer.C:
    err = TimeoutErr
  case <-ctx.Done():
    err = ctx.Err()
  case <-c.ctx.Done():
    err = c.ctx.Err()
  }
  logger.Error(err)
  return
}

func (c *Transport) Write(ctx context.Context, data net.Buffers) error {
  _, tLogger := log.WithCtx(c.ctx)
  ctx, logger := log.WithCtx(ctx)

  thisCS, err := c.connect()
  if err != nil {
    tLogger.Error(err)
    return err
  }
  idLStr := fmt.Sprintf("connid=%s", thisCS.conn.Id().String())
  tLogger.PushPrefix(idLStr)
  logger.PushPrefix(idLStr)

  _, err = thisCS.conn.WriteBuffers(data)
  if err == nil {
    return nil
  }

  // try again
  tLogger.Error(err)
  logger.Error(err)
  c.close(thisCS)

  tLogger.PopPrefix()
  logger.PopPrefix()
  logger.PushPrefix("try again,")

  thisCS, err = c.connect()
  if err != nil {
    tLogger.Error(err)
    return err
  }
  idLStr = fmt.Sprintf("connid=%s", thisCS.conn.Id().String())
  tLogger.PushPrefix(idLStr)
  logger.PushPrefix(idLStr)

  _, err = thisCS.conn.WriteBuffers(data)
  if err != nil {
    tLogger.Error(err)
    logger.Error(err)
    c.close(thisCS)
  }

  return err
}

func (c *Transport) connect() (cs *connSuit, err error) {

  c.connMutex.RLock()
  cs = c.cs
  c.connMutex.RUnlock()
  if c.cs != nil {
    return
  }

  c.connMutex.Lock()
  defer c.connMutex.Unlock()
  // read again
  if c.cs != nil {
    return c.cs, nil
  }

  ctx, logger := log.WithCtx(c.ctx)
  logger.PushPrefix("connecting...")

  conn, err := c.connector.Connect(ctx, c.addr)
  if err != nil {
    logger.Error(err)
    return nil, err
  }
  logger.PopPrefix()

  c.cs = newConnSuits(conn, conn.MaxConcurrent())

  logger.Debug(fmt.Sprintf("connected(id:%s), ", c.cs.conn.Id()))

  c.cs.conn.SetDelegate(c)

  return c.cs, nil
}

func (c *Transport) close(old *connSuit) {
  c.connMutex.Lock()
  defer c.connMutex.Unlock()
  // 已有新的连接时，说明已经关闭过了，不做任何操作
  if c.cs.conn.Id() == old.conn.Id() {
    c.cs = nil
  }

  old.close()
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
  if c.sequence == 0 {
    c.sequence = c.opt.start
  }
  c.mq[c.sequence] = ch

  return c.sequence
}

func (c *Transport) OnReceived(data []byte) {
  _, logger := log.WithCtx(c.ctx)
  seq := binary.BigEndian.Uint32(data)

  rc, ok := c.delChan(seq)
  if !ok {
    if c.opt.unknownSeq == nil {
      logger.Warning(fmt.Sprintf("not find request of reqid(%d)", seq))
    } else {
      c.opt.unknownSeq(seq, data[4:])
    }
    return
  }

  rc <- data[4:]
  close(rc)
}

func (c *Transport) OnClosed(id connid.Id) {
  c.connMutex.Lock()
  defer c.connMutex.Unlock()
  // 当前的cs
  if c.cs.conn.Id() == id {
    c.cs.close()
    c.cs = nil
  }
}
