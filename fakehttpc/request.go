package fakehttpc

import "net"

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

type Request struct {
  Header map[string]string
  Data   []byte
}

func (r *Request) Buffers() net.Buffers {
  ret := make(net.Buffers, 0, len(r.Header) + 1)

  for k, v := range r.Header {
    kl := byte(len(k))
    vl := byte(len(v))
    t := make([]byte, 2+kl+vl)
    ret = append(ret, t)
    t[0] = kl
    t = t[1:]
    copy(t, k)
    t[kl] = vl
    t = t[kl+1:]
    copy(t, v)
  }

  if r.Data == nil {
    r.Data = make([]byte, 0)
  }

  // header-end-flag and data
  ret = append(ret, []byte{0}, r.Data)

  return ret
}
