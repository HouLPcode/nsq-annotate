package nsqd

import (
	"bytes"
	"container/heap"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-diskqueue"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/pqueue"
	"github.com/nsqio/nsq/internal/quantile"
)

type Consumer interface {
	UnPause()
	Pause()
	Close() error
	TimedOutMessage() // inflight count --
	Stats() ClientStats
	Empty()
}

// Channel represents the concrete type for a NSQ channel (and also
// implements the Queue interface)
// Channel表示NSQ通道的具体类型（并且还实现了Queue接口）
// There can be multiple channels per topic, each with there own unique set
// of subscribers (clients).
// 每个topic可以有多个通道，每个通道都有自己独特的订户组（客户端）。
// Channels maintain all client and message metadata, orchestrating in-flight
// messages, timeouts, requeuing, etc.
// Channels维护所有客户端和消息元数据，编排正在进行的消息，超时，重新排队等。
type Channel struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	requeueCount uint64 // ？？？requeue次数？？？
	messageCount uint64 // 接受到的消息总数
	timeoutCount uint64 // 延迟的消息总数 在inflight中消息放入内存channel中的时候++

	sync.RWMutex

	topicName string
	name      string // channel name
	ctx       *context

	backend BackendQueue

	memoryMsgChan chan *Message // 存放消息的channel，此处称为queue
	exitFlag      int32
	exitMutex     sync.RWMutex

	// state tracking
	clients        map[int64]Consumer // 所有连接到该channel的消费者
	paused         int32
	ephemeral      bool
	deleteCallback func(*Channel)
	deleter        sync.Once

	// Stats tracking 统计跟踪
	e2eProcessingLatencyStream *quantile.Quantile

	// TODO: these can be DRYd up
	// 按照延时时间排序的小堆，最先到期的消息在堆顶
	// 线程安全的操作，
	// TODO 注意，message和PQ中的消息不是原子的操作，会有漏洞吗？？？
	// 此处的消息到期后会转移到memoryMsgChan中
	// wait for the specified timeout and requeue a message
	deferredMessages map[MessageID]*pqueue.Item // 此处的消息是map缓存的，无序。此处的map可能是用来去重的？？？？？？？
	deferredPQ       pqueue.PriorityQueue // 此处用小堆结构缓存延时消息，最先到期的消息在堆顶
	deferredMutex    sync.Mutex

	// inflight的消息会根据有没有延时，决定转入defer还是内存channel中
	// 这是要直接发送给客户端的数据
	inFlightMessages map[MessageID]*Message
	inFlightPQ       inFlightPqueue
	inFlightMutex    sync.Mutex
}

// NewChannel creates a new instance of the Channel type and returns a pointer
func NewChannel(topicName string, channelName string, ctx *context,
	deleteCallback func(*Channel)) *Channel {

	c := &Channel{
		topicName:      topicName,
		name:           channelName,
		memoryMsgChan:  make(chan *Message, ctx.nsqd.getOpts().MemQueueSize),
		clients:        make(map[int64]Consumer),
		deleteCallback: deleteCallback,
		ctx:            ctx,
	}
	// TODO ？？？
	if len(ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles) > 0 {
		c.e2eProcessingLatencyStream = quantile.New(
			ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
			ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles,
		)
	}

	c.initPQ()

	if strings.HasSuffix(channelName, "#ephemeral") {
		c.ephemeral = true
		c.backend = newDummyBackendQueue()
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := ctx.nsqd.getOpts()
			lg.Logf(opts.Logger, opts.logLevel, lg.LogLevel(level), f, args...)
		}
		// backend names, for uniqueness, automatically include the topic...
		backendName := getBackendName(topicName, channelName)
		c.backend = diskqueue.New(
			backendName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}

	c.ctx.nsqd.Notify(c)

	return c
}

// 初始化 inFligh 和 deferred
func (c *Channel) initPQ() {
	pqSize := int(math.Max(1, float64(c.ctx.nsqd.getOpts().MemQueueSize)/10))

	c.inFlightMutex.Lock()
	c.inFlightMessages = make(map[MessageID]*Message)
	c.inFlightPQ = newInFlightPqueue(pqSize)
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock()
	c.deferredMessages = make(map[MessageID]*pqueue.Item)
	c.deferredPQ = pqueue.New(pqSize)
	c.deferredMutex.Unlock()
}

// Exiting returns a boolean indicating if this channel is closed/exiting
// 正在退出或已经关闭
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}

// Delete empties the channel and closes
func (c *Channel) Delete() error {
	return c.exit(true)
}

// Close cleanly closes the Channel
func (c *Channel) Close() error {
	return c.exit(false)
}

func (c *Channel) exit(deleted bool) error {
	c.exitMutex.Lock()
	defer c.exitMutex.Unlock()

	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): deleting", c.name)

		// since we are explicitly deleting a channel (not just at system exit time)
		// de-register this from the lookupd
		// 因为我们明确地删除了一个通道（不仅仅是在系统退出时），所以从lookupd中取消注册
		c.ctx.nsqd.Notify(c)
	} else {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): closing", c.name)
	}

	// this forceably closes client connections
	// 强制断开客户端连接
	c.RLock()
	for _, client := range c.clients {
		client.Close()
	}
	c.RUnlock()

	if deleted {
		// empty the queue (deletes the backend files, too)
		c.Empty()
		return c.backend.Delete()
	}

	// write anything leftover to disk
	c.flush()
	return c.backend.Close()
}

//清空所有的消息，内存中，inflight/deferred 中， 磁盘中
func (c *Channel) Empty() error {
	c.Lock()
	defer c.Unlock()

	c.initPQ()
	for _, client := range c.clients {
		client.Empty()
	}

	for {
		select {
		case <-c.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return c.backend.Empty()
}

// flush persists all the messages in internal memory buffers to the backend
// it does not drain inflight/deferred because it is only called in Close()
// 将内存channel，和 inflight/deferred中的消息刷入磁盘
func (c *Channel) flush() error {
	var msgBuf bytes.Buffer

	if len(c.memoryMsgChan) > 0 || len(c.inFlightMessages) > 0 || len(c.deferredMessages) > 0 {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): flushing %d memory %d in-flight %d deferred messages to backend",
			c.name, len(c.memoryMsgChan), len(c.inFlightMessages), len(c.deferredMessages))
	}

	for {
		select {
		case msg := <-c.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, c.backend)
			if err != nil {
				c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

finish:
	c.inFlightMutex.Lock()
	for _, msg := range c.inFlightMessages {
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock()
	for _, item := range c.deferredMessages {
		msg := item.Value.(*Message)
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.deferredMutex.Unlock()

	return nil
}

func (c *Channel) Depth() int64 {
	return int64(len(c.memoryMsgChan)) + c.backend.Depth()
}

func (c *Channel) Pause() error {
	return c.doPause(true)
}

func (c *Channel) UnPause() error {
	return c.doPause(false)
}

func (c *Channel) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&c.paused, 1)
	} else {
		atomic.StoreInt32(&c.paused, 0)
	}

	c.RLock()
	for _, client := range c.clients {
		if pause {
			client.Pause()
		} else {
			client.UnPause()
		}
	}
	c.RUnlock()
	return nil
}

func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

// PutMessage writes a Message to the queue
// 消息写入内存channel，超出阈值写入磁盘
func (c *Channel) PutMessage(m *Message) error {
	c.RLock()
	defer c.RUnlock()
	if c.Exiting() {
		return errors.New("exiting")
	}
	err := c.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&c.messageCount, 1)
	return nil
}

func (c *Channel) put(m *Message) error {
	select {
	case c.memoryMsgChan <- m:
	default:
		b := bufferPoolGet()
		err := writeMessageToBackend(b, m, c.backend)
		bufferPoolPut(b)
		c.ctx.nsqd.SetHealth(err)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "CHANNEL(%s): failed to write message to backend - %s",
				c.name, err)
			return err
		}
	}
	return nil
}

// 此处的timeout用的就是msg中的deffered，为什么还需要两个参数？？？好像不支持用户自定义延迟时间吧？？？
func (c *Channel) PutMessageDeferred(msg *Message, timeout time.Duration) {
	atomic.AddUint64(&c.messageCount, 1)
	c.StartDeferredTimeout(msg, timeout)
}

// TouchMessage resets the timeout for an in-flight message 。
// 更新 infer中消息延时时间，从当前时间再延时clientMsgTimeout
// 重置传播途中的消息超时时间
func (c *Channel) TouchMessage(clientID int64, id MessageID, clientMsgTimeout time.Duration) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)

	newTimeout := time.Now().Add(clientMsgTimeout)
	if newTimeout.Sub(msg.deliveryTS) >=
		c.ctx.nsqd.getOpts().MaxMsgTimeout {
		// we would have gone over, set to the max
		newTimeout = msg.deliveryTS.Add(c.ctx.nsqd.getOpts().MaxMsgTimeout)
	}

	msg.pri = newTimeout.UnixNano()
	err = c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}

// FinishMessage successfully discards an in-flight message
// 安全的删除 in-flight message
// 删除 InFlight map 和 堆中的该消息
func (c *Channel) FinishMessage(clientID int64, id MessageID) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	// TODO 这里是干嘛用的？？？
	if c.e2eProcessingLatencyStream != nil {
		c.e2eProcessingLatencyStream.Insert(msg.Timestamp)
	}
	return nil
}

// RequeueMessage requeues a message based on `time.Duration`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message (aka "deferred requeue")
//
// 从 inflight中取出消息， 有延时则放入defer中，没有直接放入内存channel中
func (c *Channel) RequeueMessage(clientID int64, id MessageID, timeout time.Duration) error {
	// remove from inflight first
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	// 更新requeueCount计数
	atomic.AddUint64(&c.requeueCount, 1)

	if timeout == 0 {
		c.exitMutex.RLock()
		if c.Exiting() {
			c.exitMutex.RUnlock()
			return errors.New("exiting")
		}
		err := c.put(msg)
		c.exitMutex.RUnlock()
		return err
	}

	// deferred requeue
	return c.StartDeferredTimeout(msg, timeout)
}

// AddClient adds a client to the Channel's client list
func (c *Channel) AddClient(clientID int64, client Consumer) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if ok {
		return
	}
	c.clients[clientID] = client
}

// RemoveClient removes a client from the Channel's client list
func (c *Channel) RemoveClient(clientID int64) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if !ok {
		return
	}
	delete(c.clients, clientID)

	if len(c.clients) == 0 && c.ephemeral == true {
		go c.deleter.Do(func() { c.deleteCallback(c) })
	}
}

// 构建消息，放入inflight map 和 堆中
func (c *Channel) StartInFlightTimeout(msg *Message, clientID int64, timeout time.Duration) error {
	now := time.Now()
	msg.clientID = clientID
	msg.deliveryTS = now
	msg.pri = now.Add(timeout).UnixNano()
	err := c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}
// 构建消息，放入 defer map 和 堆中
func (c *Channel) StartDeferredTimeout(msg *Message, timeout time.Duration) error {
	absTs := time.Now().Add(timeout).UnixNano()
	item := &pqueue.Item{Value: msg, Priority: absTs}
	// todo 注意此处defer和deferPQ的操作不是原子的
	err := c.pushDeferredMessage(item)
	if err != nil {
		return err
	}
	c.addToDeferredPQ(item)
	return nil
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
// 原子性的将msg插入inFlight map中，有去重效果
func (c *Channel) pushInFlightMessage(msg *Message) error {
	c.inFlightMutex.Lock()
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		c.inFlightMutex.Unlock()
		return errors.New("ID already in flight")
	}
	c.inFlightMessages[msg.ID] = msg
	c.inFlightMutex.Unlock()
	return nil
}

// popInFlightMessage atomically removes a message from the in-flight dictionary
// 从inflight map 中取出msgID的消息，校验clientID
func (c *Channel) popInFlightMessage(clientID int64, id MessageID) (*Message, error) {
	c.inFlightMutex.Lock()
	msg, ok := c.inFlightMessages[id]
	if !ok {
		c.inFlightMutex.Unlock()
		return nil, errors.New("ID not in flight")
	}
	if msg.clientID != clientID {
		c.inFlightMutex.Unlock()
		return nil, errors.New("client does not own message")
	}
	delete(c.inFlightMessages, id)
	c.inFlightMutex.Unlock()
	return msg, nil
}

func (c *Channel) addToInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	c.inFlightPQ.Push(msg)
	c.inFlightMutex.Unlock()
}

// 从堆中删除该消息
func (c *Channel) removeFromInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	if msg.index == -1 {
		// this item has already been popped off the pqueue
		c.inFlightMutex.Unlock()
		return
	}
	c.inFlightPQ.Remove(msg.index)
	c.inFlightMutex.Unlock()
}

// 将消息放入defer map中，按照messageID去重
func (c *Channel) pushDeferredMessage(item *pqueue.Item) error {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	id := item.Value.(*Message).ID
	// 去重处理
	_, ok := c.deferredMessages[id]
	if ok {
		c.deferredMutex.Unlock()
		return errors.New("ID already deferred")
	}
	c.deferredMessages[id] = item
	c.deferredMutex.Unlock()
	return nil
}

// 从DeferredMessage获取指定messageID的消息
func (c *Channel) popDeferredMessage(id MessageID) (*pqueue.Item, error) {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	item, ok := c.deferredMessages[id]
	if !ok {
		c.deferredMutex.Unlock()
		return nil, errors.New("ID not deferred")
	}
	delete(c.deferredMessages, id)
	c.deferredMutex.Unlock()
	return item, nil
}

// 将消息放入defer小堆中
func (c *Channel) addToDeferredPQ(item *pqueue.Item) {
	c.deferredMutex.Lock()
	heap.Push(&c.deferredPQ, item)
	c.deferredMutex.Unlock()
}

// 将所有小于t的消息从deferredPQ和DeferredMessage中移除，存储到内存channel中（memoryMsgChan）
func (c *Channel) processDeferredQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		// 获取到小于t的消息
		c.deferredMutex.Lock()
		item, _ := c.deferredPQ.PeekAndShift(t)
		c.deferredMutex.Unlock()

		if item == nil {
			goto exit
		}
		dirty = true

		// 从DeferredMessage中删除该消息
		msg := item.Value.(*Message)
		_, err := c.popDeferredMessage(msg.ID)
		if err != nil {
			goto exit
		}
		// 将消息放入内存消息中
		c.put(msg)
	}

exit:
	return dirty
}
// 将所有小于t的消息从infligfht map 和 堆中取出消息，放入内存channel中。
// 调用了一次client.TimedOutMessage()，不知道时做什么用的
func (c *Channel) processInFlightQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.inFlightMutex.Lock()
		msg, _ := c.inFlightPQ.PeekAndShift(t)
		c.inFlightMutex.Unlock()

		if msg == nil {
			goto exit
		}
		dirty = true

		_, err := c.popInFlightMessage(msg.clientID, msg.ID)
		if err != nil {
			goto exit
		}
		// 此处增加timeoutCount计数
		atomic.AddUint64(&c.timeoutCount, 1)
		c.RLock()
		client, ok := c.clients[msg.clientID]
		c.RUnlock()
		if ok {
			// TODO 这是什么操作？？？
			client.TimedOutMessage()
		}
		// 放入内存 channel 中
		c.put(msg)
	}

exit:
	return dirty
}
