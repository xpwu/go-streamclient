package fakehttpc

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

type Status byte

const (
  Success   Status   = 0
  Failed       = 1
)

type Response struct {
  Status Status
  Data   []byte
}

func NewResponse(res []byte) *Response {
  ret := &Response{}

  ret.Status = Status(res[0])
  ret.Data = res[1:]

  return ret
}

