package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/app"
	"github.com/nsqio/nsq/internal/version"
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	channel       = flag.String("channel", "", "NSQ channel")
	maxInFlight   = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")
	totalMessages = flag.Int("n", 0, "total messages to show (will wait if starved)")
	printTopic    = flag.Bool("print-topic", false, "print topic name where message was received")

	nsqdTCPAddrs     = app.StringArray{}
	lookupdHTTPAddrs = app.StringArray{}
	topics           = app.StringArray{}
)

func init() {
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flag.Var(&topics, "topic", "NSQ topic (may be given multiple times)")
}

type TailHandler struct {
	topicName     string
	totalMessages int
	messagesShown int
}

func (th *TailHandler) HandleMessage(m *nsq.Message) error {
	th.messagesShown++

	if *printTopic {
		_, err := os.Stdout.WriteString(th.topicName)
		if err != nil {
			log.Fatalf("ERROR: failed to write to os.Stdout - %s", err)
		}
		_, err = os.Stdout.WriteString(" | ")
		if err != nil {
			log.Fatalf("ERROR: failed to write to os.Stdout - %s", err)
		}
	}

	// 从消息中读取数据内容
	_, err := os.Stdout.Write(m.Body)
	if err != nil {
		log.Fatalf("ERROR: failed to write to os.Stdout - %s", err)
	}
	_, err = os.Stdout.WriteString("\n")
	if err != nil {
		log.Fatalf("ERROR: failed to write to os.Stdout - %s", err)
	}
	if th.totalMessages > 0 && th.messagesShown >= th.totalMessages {
		os.Exit(0)
	}
	return nil
}

// 消费者
// 使用流程

//func (){
//	cfg := nsq.NewConfig()
//  1. 创建消费者
//	consumer, err := nsq.NewConsumer(topics, channel, cfg)
//  2. 绑定消息处理函数，此处是一个实现了Handler接口的结构
//	consumer.AddHandler(&TailHandler{}) // Handler interface{}
//  3. 通过lookup连接到nsqd网络
//	err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs) // 或者err = consumer.ConnectToNSQDs(nsqdTCPAddrs)，不建议使用？？？
//	wait...
//	consumer.Stop()
//  <-consumer.StopChan // 消费者stop后需要的操作
//}

// 必须指定的参数 lookupdHTTPAddrs或nsqdTCPAddrs，topics，
func main() {
	cfg := nsq.NewConfig()

	flag.Var(&nsq.ConfigFlag{cfg}, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_tail v%s\n", version.Binary)
		return
	}

	if *channel == "" {
		rand.Seed(time.Now().UnixNano())
		*channel = fmt.Sprintf("tail%06d#ephemeral", rand.Int()%999999)
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatal("--nsqd-tcp-address or --lookupd-http-address required")
	}
	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Fatal("use --nsqd-tcp-address or --lookupd-http-address not both")
	}
	if len(topics) == 0 {
		log.Fatal("--topic required")
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Don't ask for more messages than we want
	if *totalMessages > 0 && *totalMessages < *maxInFlight {
		*maxInFlight = *totalMessages
	}

	cfg.UserAgent = fmt.Sprintf("nsq_tail/%s go-nsq/%s", version.Binary, nsq.VERSION)
	cfg.MaxInFlight = *maxInFlight

	consumers := []*nsq.Consumer{}
	for i := 0; i < len(topics); i += 1 {
		log.Printf("Adding consumer for topic: %s\n", topics[i])
		// 创建消费者，绑定topic，但是channel是自己指定的
		consumer, err := nsq.NewConsumer(topics[i], *channel, cfg)
		if err != nil {
			log.Fatal(err)
		}
		// 添加处理函数
		consumer.AddHandler(&TailHandler{topicName: topics[i], totalMessages: *totalMessages})
		// 连接，可以是空的
		err = consumer.ConnectToNSQDs(nsqdTCPAddrs)
		if err != nil {
			log.Fatal(err)
		}
		// 连接。。。
		err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
		if err != nil {
			log.Fatal(err)
		}

		consumers = append(consumers, consumer)
	}

	<-sigChan
	// 关闭消费者的步骤 1. stop 2. 读取consumer.StopChan通道
	for _, consumer := range consumers {
		consumer.Stop()
	}
	for _, consumer := range consumers {
		<-consumer.StopChan
	}
}
