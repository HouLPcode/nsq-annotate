// This is an NSQ client that publishes incoming messages from
// stdin to the specified topic.
// 生产者，通过令牌算法限流
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/app"
	"github.com/nsqio/nsq/internal/version"
)

var (
	topic     = flag.String("topic", "", "NSQ topic to publish to")
	// 分隔符
	delimiter = flag.String("delimiter", "\n", "character to split input from stdin")

	destNsqdTCPAddrs = app.StringArray{}
)

func init() {
	flag.Var(&destNsqdTCPAddrs, "nsqd-tcp-address", "destination nsqd TCP address (may be given multiple times)")
}

// 生产者
// 使用流程
//func (){
//	cfg := nsq.NewConfig()
//	producer, err := nsq.NewProducer(addr, cfg)
//	err := producer.Publish(*topic, datas)
//  producer.Stop()
//}
func main() {
	cfg := nsq.NewConfig()
	flag.Var(&nsq.ConfigFlag{cfg}, "producer-opt", "option to passthrough to nsq.Producer (may be given multiple times, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	// 限制每秒发送几条消息，0表示不限制
	rate := flag.Int64("rate", 0, "Throttle messages to n/second. 0 to disable")

	flag.Parse()

	if len(*topic) == 0 {
		log.Fatal("--topic required")
	}

	if len(*delimiter) != 1 {
		log.Fatal("--delimiter must be a single byte")
	}

	stopChan := make(chan bool) // 无缓冲channel，关闭的时候退出阻塞
	// 监听系统信号
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	cfg.UserAgent = fmt.Sprintf("to_nsq/%s go-nsq/%s", version.Binary, nsq.VERSION)

	// make the producers
	producers := make(map[string]*nsq.Producer) // addr --> producer
	for _, addr := range destNsqdTCPAddrs {
		// 根据nsqd的ip地址常见生产者
		producer, err := nsq.NewProducer(addr, cfg)
		if err != nil {
			log.Fatalf("failed to create nsq.Producer - %s", err)
		}
		producers[addr] = producer
	}

	if len(producers) == 0 {
		log.Fatal("--nsqd-tcp-address required")
	}

	throttleEnabled := *rate >= 1 // 0 表示不限制消息的发送
	balance := int64(1) // 令牌桶限流算法
	// avoid divide by 0 if !throttleEnabled
	var interval time.Duration
	if throttleEnabled {
		interval = time.Second / time.Duration(*rate) // 间隔一段时间发送消息
	}
	// 开启新的协程，用来限制消息的发送速率
	go func() {
		if !throttleEnabled { // 不限速直接退出协程
			return
		}
		log.Printf("Throttling messages rate to max:%d/second", *rate)
		// every tick increase the number of messages we can send
		// 每隔固定时间，生成一个令牌环，最多rate个
		for _ = range time.Tick(interval) {
			n := atomic.AddInt64(&balance, 1)
			// if we build up more than 1s of capacity just bound to that
			if n > int64(*rate) {
				atomic.StoreInt64(&balance, int64(*rate))
			}
		}
	}()

	r := bufio.NewReader(os.Stdin)
	delim := (*delimiter)[0] // 分割符的第一个字符
	go func() {
		for {
			var err error
			if throttleEnabled {
				// 获取令牌，没有则等待一个固定时间，这段时间上面的协程会生成一个令牌
				currentBalance := atomic.LoadInt64(&balance)
				if currentBalance <= 0 { // 没有令牌则等待
					time.Sleep(interval)
				}
				// 此处不用再次获取令牌，因为每隔固定时间上面的协程肯定会生成一个令牌
				err = readAndPublish(r, delim, producers)
				atomic.AddInt64(&balance, -1) // -1，消耗一个令牌
			} else {
				// 不限流直接发送
				err = readAndPublish(r, delim, producers)
			}
			// 文件读取结束或者发送消息失败
			if err != nil {
				if err != io.EOF {
					log.Fatal(err)
				}
				close(stopChan)
				break
			}
		}
	}()

	// 阻塞等待中断或退出信号
	select {
	case <-termChan:
	case <-stopChan: // 无缓冲channel，关闭的时候退出阻塞
	}

	// 退出前停止所有生产者
	for _, producer := range producers {
		producer.Stop()
	}
}

// readAndPublish reads to the delim from r and publishes the bytes
// to the map of producers.
// 从r中读取数据，遇见delim停止读取，数据发送给所有nsqd中，topic。 此处的生产者是环境中的nsqd
func readAndPublish(r *bufio.Reader, delim byte, producers map[string]*nsq.Producer) error {
	line, readErr := r.ReadBytes(delim)

	if len(line) > 0 {
		// trim the delimiter
		line = line[:len(line)-1] // 去掉停止符
	}
	// 没有读取到数据直接返回
	if len(line) == 0 {
		return readErr
	}

	for _, producer := range producers {
		// 往固定topic 上发送消息
		err := producer.Publish(*topic, line)
		if err != nil {
			return err
		}
	}

	return readErr
}
