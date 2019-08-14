package nsqd

import (
	"io"
	"net"

	"github.com/nsqio/nsq/internal/protocol"
)

type tcpServer struct {
	ctx *context
}

// 具体的tcp链接处理函数
func (p *tcpServer) Handle(clientConn net.Conn) {
	p.ctx.nsqd.logf(LOG_INFO, "TCP: new client(%s)", clientConn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	// 客户端首先发送4字节协议版本，协商协议版本
	buf := make([]byte, 4)
	_, err := io.ReadFull(clientConn, buf) //读取4B数据
	if err != nil {
		p.ctx.nsqd.logf(LOG_ERROR, "failed to read protocol version - %s", err)
		return
	}
	protocolMagic := string(buf)

	p.ctx.nsqd.logf(LOG_INFO, "CLIENT(%s): desired protocol magic '%s'",
		clientConn.RemoteAddr(), protocolMagic)

	var prot protocol.Protocol
	switch protocolMagic {
	// 目前只支持V2版本
	case "  V2":
		prot = &protocolV2{ctx: p.ctx}
	default:
		// 不支持的版本输出日志，断开链接
		protocol.SendFramedResponse(clientConn, frameTypeError, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		p.ctx.nsqd.logf(LOG_ERROR, "client(%s) bad protocol magic '%s'",
			clientConn.RemoteAddr(), protocolMagic)
		return
	}

	err = prot.IOLoop(clientConn)
	if err != nil {
		p.ctx.nsqd.logf(LOG_ERROR, "client(%s) - %s", clientConn.RemoteAddr(), err)
		return
	}
}
