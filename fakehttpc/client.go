package fakehttpc

import (
  "context"
  "errors"
  "fmt"
  "github.com/xpwu/go-log/log"
  "github.com/xpwu/go-reqid/reqid"
  "github.com/xpwu/go-streamclient/transport"
  "time"
)

type Client struct {
  transport *transport.Transport
  push      chan []byte
}

type delegate struct {
  client *Client
}

func NewClient(ctx context.Context, addr string, connector transport.Connector) *Client {
  ret := &Client{
    transport: transport.New(ctx, connector, addr, transport.StartSeq(10)),
    push:      make(chan []byte, 20),
  }
  ret.transport.Delegate = &delegate{ret}

  return ret
}

func (d *delegate) UnknownSeq(seq uint32, data []byte) {
  ctx, logger := log.WithCtx(d.client.transport.Context())

  if seq != PushReqId {
    logger.Warning(fmt.Sprintf("not find request of reqid(%d)", seq))
    return
  }

  push := NewPush(data)
  logger.Debug(fmt.Sprintf("receive push data: id(%d), len(%d).", push.Id(), len(push.Data)))
  // 超过buffer的数量，自动丢弃
  // 之所以不使用回调方式，是因为上层收到数据后一样要做goroutine的切换，另外上层如果有同步操作，还会阻塞这里的执行
  select {
  case d.client.push <- push.Data:
  default:
  }

  logger.PushPrefix("push ack,")
  err := d.client.transport.Write(ctx, PushAck(push.ID))
  if err != nil {
    logger.Error(err)
  }
}

func (c *Client) ReadPush(ctx context.Context) (d []byte, err error) {
  ctx, logger := log.WithCtx(ctx)
  logger.PushPrefix("read push data,")

  select {
  case d = <-c.push:
    logger.Debug(fmt.Sprintf("return data(len=%d)", len(d)))
    return
  case <-ctx.Done():
    err = ctx.Err()
    logger.Error(err)
    return
  }
}

func (c *Client) Send(ctx context.Context, headers map[string]string, body []byte,
  timeout time.Duration) (response []byte, err error) {

  ctx, reqId := reqid.WithCtx(ctx)
  ctx, logger := log.WithCtx(ctx)

  logger.PushPrefix(fmt.Sprintf("send Header-Reqid(%s),", reqId))

  logger.Debug("start")

  if body == nil {
    body = make([]byte, 0)
  }

  if headers == nil {
    headers = make(map[string]string)
  }
  headers[reqid.HeaderKey] = reqId

  r := &Request{}
  r.Data = body
  r.Header = headers

  resD, err := c.transport.Send(ctx, r.Buffers(), timeout)

  if err != nil {
    logger.Error(err)
    return
  }

  res := NewResponse(resD)

  if res.Status != Success {
    err = errors.New(string(res.Data))
    logger.Error(err)
    return
  }

  logger.Debug("end")

  return res.Data, nil
}
