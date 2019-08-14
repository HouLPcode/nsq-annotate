package protocol

import (
	"encoding/binary"
	"io"
	"net"
)

// Protocol describes the basic behavior of any protocol in the system
type Protocol interface {
	IOLoop(conn net.Conn) error
}

// SendResponse is a server side utility function to prefix data with a length header
// and write to the supplied Writer
// 写入数据，格式 长度+数据
func SendResponse(w io.Writer, data []byte) (int, error) {
	err := binary.Write(w, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, err
	}

	n, err := w.Write(data)
	if err != nil {
		return 0, err
	}

	return (n + 4), nil
}

// SendFramedResponse is a server side utility function to prefix data with a length header
// and frame header and write to the supplied Writer
// 服务端调用，数据帧格式 [(4B)消息长度 , (4byte)消息类型, (载体)] ，返回写入的数据总长度
func SendFramedResponse(w io.Writer, frameType int32, data []byte) (int, error) {
	beBuf := make([]byte, 4)
	size := uint32(len(data)) + 4

	binary.BigEndian.PutUint32(beBuf, size)
	n, err := w.Write(beBuf) // 写入消息长度，data长度+4，大端
	if err != nil {
		return n, err
	}

	binary.BigEndian.PutUint32(beBuf, uint32(frameType))
	n, err = w.Write(beBuf) // 写入消息类型
	if err != nil {
		return n + 4, err
	}

	n, err = w.Write(data) // 写入消息数据
	return n + 8, err
}
