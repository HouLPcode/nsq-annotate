package main

import "github.com/nsqio/go-nsq"

func main() {
	// 消费者客户端代码
	cfg := nsq.NewConfig()
	//1. 创建消费者
	consumer, _ := nsq.NewConsumer(topics, channel, cfg)
	//2. 绑定消息处理函数，此处是一个实现了Handler接口的结构
	consumer.AddHandler(&TailHandler{}) // Handler interface{}
	//3. 通过lookup连接到nsqd网络
	consumer.ConnectToNSQLookupds([]string{"127.0.0.1:4161"}) // 或者err = consumer.ConnectToNSQDs(nsqdTCPAddrs)，不建议使用？？？

	//wait...

	consumer.Stop()
	<-consumer.StopChan // 消费者stop后需要的操作
}
