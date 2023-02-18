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
}

type Connector interface {
	Connect(ctx context.Context, addr string) (conn Conn, err error)
}

type Transport struct {
	connector  Connector
	conn       Conn
	connClosed chan struct{}
	mutex      sync.RWMutex
	addr       string
	ctx        context.Context

	mq       map[uint32]chan []byte
	sequence uint32
	mqMu     sync.Mutex
}

func New(ctx context.Context, connector Connector, addr string) *Transport {
	return &Transport{
		connector:  connector,
		conn:       nil,
		connClosed: nil,
		mutex:      sync.RWMutex{},
		addr:       addr,
		ctx:        ctx,
		mq:         make(map[uint32]chan []byte),
		sequence:   0,
		mqMu:       sync.Mutex{},
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

	thisId, err := c.connect()
	if err != nil {
		tLogger.Error(err)
		return nil, err
	}
	idLStr := fmt.Sprintf("connid=%s", thisId.String())
	tLogger.PushPrefix(idLStr)
	logger.PushPrefix(idLStr)

	resCh := make(chan []byte)
	seq := c.addChan(resCh)

	buffers := make([][]byte, 1, 1 + len(data))
	buffers[0] = make([]byte, sequenceLen)
	binary.BigEndian.PutUint32(buffers[0], seq)
	buffers = append(buffers, data...)

	_, err = c.conn.WriteBuffers(buffers)
	if err != nil {
		tLogger.Error(err)
		logger.Error(err)
		c.delChan(seq)
		c.close(thisId)
		return
	}

	select {
	case res = <-resCh:
		logger.Debug("succeed")
		return

	case <-c.connClosed:
		err = errors.New("connection closed")
	case <-timer.C:
		err = TimeoutErr
	case <-ctx.Done():
		err = ctx.Err()
	case <-c.ctx.Done():
		err = c.ctx.Err()
	}
	c.delChan(seq)
	logger.Error(err)
	return
}

func (c *Transport) connect() (id connid.Id, err error) {

	c.mutex.RLock()
	if c.conn != nil {
		id = c.conn.Id()
		c.mutex.RUnlock()
		return
	}
	c.mutex.RUnlock()

	c.mutex.Lock()
	defer c.mutex.Unlock()
	// read again
	if c.conn != nil {
		return c.conn.Id(), nil
	}

	ctx, logger := log.WithCtx(c.ctx)
	logger.PushPrefix("connecting...")

	conn, err := c.connector.Connect(ctx, c.addr)
	if err != nil {
		logger.Error(err)
		return 0, err
	}
	logger.PopPrefix()

	c.conn = conn
	c.connClosed = make(chan struct{})

	logger.Debug(fmt.Sprintf("connected(id:%s), ", c.conn.Id()))

	c.conn.SetDelegate(c)

	return c.conn.Id(), nil
}

func (c *Transport) close(oldId connid.Id) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// 已有新的连接时，说明已经关闭过了，不做任何操作
	if c.conn == nil || oldId != c.conn.Id() {
		return
	}

	_ = c.conn.Close()
	close(c.connClosed)
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

func (c *Transport) OnReceived(data []byte) {
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

func (c *Transport) OnClosed(id connid.Id) {
	c.close(id)
}
