package consumer

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

type ConsumerConf struct {
	Handler     func(msg string) (newMsg string) // 处理消息的函数
	FailPushMsh func(msg string)                 // 失败消息重新投递
	MinWorker   int                              // 最小worker数量
	MaxWorker   int                              // 最大worker数量
	L           *zap.Logger                      // 日志
}

var (
	conf      *ConsumerConf
	MsgChan   chan string
	ctx       context.Context
	cancel    context.CancelFunc
	consumers = make(map[*Consumer]bool)
	mu        sync.Mutex
)

// 初始化消费者
func InitConsumer(consumerConf *ConsumerConf) {
	conf = consumerConf
	MsgChan = make(chan string, 100)
	ctx, cancel = context.WithCancel(context.Background())
	go start()
}

// 投递消息
func PushMsg(msg string) {
	MsgChan <- msg
}

// 启动消费者
func start() {
	defer func() { //异常退出是 重新启动
		if r := recover(); r != nil {
			conf.L.Error("consumer.start stop error ", zap.Any("error", r))
			// 重新启动
			go start()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			sleepTime := manageConsumer()
			time.Sleep(sleepTime) // 每1秒检查一次
		}
	}
}

// 管理消费者
func manageConsumer() time.Duration {
	mu.Lock()
	defer mu.Unlock()
	sleepTime := time.Second * 1

	consumerLength := GetConsumeLen()
	if consumerLength < conf.MinWorker { //默认启动一个协程
		diff := conf.MinWorker - consumerLength
		startMultiConsumer(diff)
		return sleepTime
	}

	length := GetChanMsgLen()

	if length >= 5 && consumerLength < conf.MaxWorker { //启动一个消费协程
		startMultiConsumer(50)
	} else if length < 1 && consumerLength > conf.MaxWorker { //关闭一个消费协程
		diff := consumerLength - conf.MaxWorker
		if diff > 50 {
			diff = 50
		}
		stopMultiConsumer(diff)
		sleepTime = time.Second * 5
	}

	return sleepTime
}

// 启动多个消费者
func startMultiConsumer(num int) {
	for i := 0; i < num; i++ {
		consumer := NewConsumer(GetConsumeLen())
		consumers[consumer] = true
		go consumer.Start()
	}
}

// 关闭多个消费者
func stopMultiConsumer(num int) {
	i := 0
	for consumer := range consumers {
		consumer.Stop()
		i++
		if i >= num {
			break
		}
	}
}

// 关闭所有消费者
func stopAllConsumer() {
	mu.Lock()
	for consumer := range consumers {
		consumer.Stop()
	}
	mu.Unlock()

	//睡100毫秒
	time.Sleep(time.Millisecond * 100)
	for {
		mu.Lock()
		len := len(consumers)
		mu.Unlock()
		if len == 0 {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
}

// 获取消费者数量
func GetConsumeLen() int {
	return len(consumers)
}

// 获取chan消息数量
func GetChanMsgLen() int {
	return len(MsgChan)
}

// 删除消费者
func deleteConsumer(c *Consumer) {
	mu.Lock()
	delete(consumers, c)
	num := len(consumers)
	mu.Unlock()
	conf.L.Info("Consumer.consumeMsg stop ", zap.Int("idx", c.idx), zap.Int("num", num))
}

// 停止消费者
func Stop(rePushMsg func(msg string)) {
	defer func() {
		conf.L.Info("Consumer.Stop stop all consumer")
		if r := recover(); r != nil {
			conf.L.Error("consumer.start stop error ", zap.Any("error", r))
			// 重新启动
			go start()
		}
	}()
	cancel()

	stopAllConsumer()

	// 重新投递消息
	for msg := range MsgChan {
		rePushMsg(msg)
	}

}
