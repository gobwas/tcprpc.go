package tcprpc

import (
	"encoding/json"
	"net"
	"bufio"
	"github.com/nu7hatch/gouuid"
	"log"
)

const Delimiter = byte('\n')

type Request struct {
	Id string			 `json:"id"`
	Topic string		 `json:"topic"`
	Params []interface{} `json:"params"`
}

type Response interface {}

type Success struct {
	Id string
	Result interface{}
}

type ErrorDescription struct {
	Code float64
	Message string
}

type Error struct {
	Id string
	Error ErrorDescription
}

type Client struct {
	socket net.Conn
	address string
	requests chan Request
	responses map[string]chan interface{}
}

func (c Client) Request(topic string, params []interface{}) (r Response, err error) {
	u4, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}

	id := u4.String()

	req := Request {
		Id: id,
		Topic: topic,
		Params: params,
	}

	c.requests <- req

	succC := make(chan interface{})
	c.responses[id] = succC;

	resp := <- succC

	return resp, nil
}

func pushToSocket(client Client) {
	for {
		request := <- client.requests

		encoded, err := json.Marshal(request)
		if err != nil {
			log.Fatal(err)
		}

		client.socket.Write(encoded)
		client.socket.Write([]byte{Delimiter})
	}
}

func readFromSocket(client Client) {
	reader := bufio.NewReader(client.socket)

	for {
		str, err := reader.ReadString(Delimiter)
		if err != nil {
			log.Fatal(err)
		}

		if len(str) > 0 {
			var obj map[string]interface{}
			err := json.Unmarshal([]byte(str), &obj)
			if err != nil {
				log.Fatal(err)
			}

			// determine which type of is response
			if error, ok := obj["error"].(map[string]interface{}); ok {
				resp := Error {
					Id: obj["id"].(string),
					Error: ErrorDescription {
						Code: error["code"].(float64),
						Message: error["message"].(string),
					},
				}

				client.responses[obj["id"].(string)] <- resp
			} else {
				resp := Success {
					Id: obj["id"].(string),
					Result: obj["result"],
				}

				client.responses[obj["id"].(string)] <- resp
			}
		}

//		if readLen > 0 {
//			hunk := string(readBuf[:readLen])
//			parts := strings.Split(hunk, string(Delimiter))
//			length := len(parts)
//
//			// there is no delimiter in hunk
//			if length == 1 {
//				buffer = append(buffer, hunk)
//				continue reading
//			}
//
//			for index, part := range parts {
//				buffer = append(buffer, part);
//
//				// this is the last chunk
//				// so we need to get the next parts
//				// to find delimiter there
//				if (index + 1) == length {
//					continue reading
//				}
//
//				joined := strings.Join(buffer, "")
//				buffer = buffer[:0]
//
//				var obj map[string]interface{}
//				err := json.Unmarshal([]byte(joined), &obj)
//				if err != nil {
//					log.Fatal(err)
//				}
//
//				// determine which type of is response
//				if error, ok := obj["error"].(map[string]interface{}); ok {
//					resp := Error {
//						Id: obj["id"].(string),
//						Error: ErrorDescription {
//							Code: error["code"].(float64),
//							Message: error["message"].(string),
//						},
//					}
//
//					client.responses[obj["id"].(string)] <- resp
//				} else {
//					resp := Success {
//						Id: obj["id"].(string),
//						Result: obj["result"],
//					}
//
//					client.responses[obj["id"].(string)] <- resp
//				}
//			}
//
//
//		}
	}
}

func NewClient(address string) Client {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatal(err)
	}

	c := Client {
		address: address,
		requests: make(chan Request),
		responses: make(map[string]chan interface{}),
		socket: conn,
	}

	go readFromSocket(c)
	go pushToSocket(c)

	return c
}



