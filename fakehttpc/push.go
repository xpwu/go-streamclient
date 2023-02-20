package fakehttpc

import (
  "encoding/binary"
  "net"
)

/**

content protocol:
     request ---
       reqid | headers | header-end-flag | data
         reqid: 4 bytes, net order;
         headers: < key-len | key | value-len | value > ... ;  [optional]
           key-len: 1 byte,  key-len = sizeof(key);
           value-len: 1 byte, value-len = sizeof(value);
         header-end-flag: 1 byte, === 0;
         data:       [optional]

		reqid = 1: client push ack to server.
					ack: no headers;
					data: pushId. 4 bytes, net order;

  ---------------------------------------------------------------------
     response ---
       reqid | status | data
         reqid: 4 bytes, net order;
         status: 1 byte, 0---success, 1---failed
         data: if status==success, data=<app data>    [optional]
               if status==failed, data=<error reason>


     reqid = 1: server push to client
				status: 0
				data: first 4 bytes --- pushId, net order;
							last --- real data

*/

/**
常规请求的 reqid 使用 transport 处理，推送的由client处理
*/

const (
  PushReqId uint32 = 1
)

type Push struct {
  ID []byte
  Data []byte
}

func (p *Push) Id() uint32 {
  return binary.BigEndian.Uint32(p.ID)
}

func NewPush(d []byte) *Push {
  return &Push{
    ID: d[:4],
    Data: d[4:],
  }
}

func PushAck(id []byte) net.Buffers {
  ret := make([]byte, 5)
  binary.BigEndian.PutUint32(ret, PushReqId)
  ret[4] = 0

  buffer := make(net.Buffers, 2)
  buffer[0] = ret
  buffer[1] = id

  return buffer
}
