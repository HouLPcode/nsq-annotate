package protocol

import (
	"net"
	"runtime"
	"strings"

	"github.com/nsqio/nsq/internal/lg"
)

type TCPHandler interface {
	Handle(net.Conn)
}

//   监听器       处理器      日志
func TCPServer(listener net.Listener, handler TCPHandler, logf lg.AppLogFunc) {
	logf(lg.INFO, "TCP: listening on %s", listener.Addr())

	for {
		clientConn, err := listener.Accept() // 阻塞等待新的链接到来
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				logf(lg.WARN, "temporary Accept() failure - %s", err)
				runtime.Gosched()
				continue
			}
			// theres no direct way to detect this error because it is not exposed
			if !strings.Contains(err.Error(), "use of closed network connection") {
				logf(lg.ERROR, "listener.Accept() - %s", err)
			}
			break
		}
		go handler.Handle(clientConn) // 每个新连接创建goroutine处理
	}

	logf(lg.INFO, "TCP: closing %s", listener.Addr())
}
