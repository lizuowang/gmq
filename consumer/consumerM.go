package consumer

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type ConsumerConf struct {
	Handler     func(msg string) (newMsg string) // 处理消息的函数
	FailPushMsh func(msg string)                 // 失败消息重新投递
	MinWorker   int                              // 最小worker数量
	MaxWorker   int                              // 最大worker数量
	AddWorker   int                              // 每次启动的协程数量
	WaitNum     int                              // 等待消息数量 大于这个数量 启动协程
	FreeTimes   int                              // 空闲次数 大于这个数量 关闭协程
	L           *zap.Logger                      // 日志
}

var (
	conf      *ConsumerConf
	MsgChan   chan string
	ctx       context.Context
	cancel    context.CancelFunc
	consumers = make(map[*Consumer]bool)
	mu        sync.Mutex
	freeTimes = 0   // 空闲次数
	freeCNum  int32 // 空闲协程数量
)

// 增加空闲协程数量
func IncrFreeCNum() {
	atomic.AddInt32(&freeCNum, 1)
}

// 减少空闲协程数量
func DecrFreeCNum() {
	atomic.AddInt32(&freeCNum, -1)
}

// 获取空闲协程数量
func GetFreeCNum() int32 {
	return atomic.LoadInt32(&freeCNum)
}

// 初始化消费者
func InitConsumer(consumerConf *ConsumerConf) {

	if consumerConf == nil {
		log.Fatalf("consumer 配置错误")
	}
	if consumerConf.Handler == nil {
		log.Fatalf("consumer ConsumerConf.Handler 不能为空")
	}
	if consumerConf.MinWorker <= 0 {
		log.Fatalf("consumer ConsumerConf.MinWorker 不能小于等于0")
	}
	if consumerConf.MaxWorker <= 0 {
		log.Fatalf("consumer ConsumerConf.MaxWorker 不能为空")
	}
	if consumerConf.L == nil {
		log.Fatalf("consumer ConsumerConf.L 不能为空")
	}
	// 等待消息数量
	if consumerConf.WaitNum <= 0 {
		consumerConf.WaitNum = 2
	}
	// 每次启动的协程数量
	if consumerConf.AddWorker <= 0 {
		consumerConf.AddWorker = consumerConf.MinWorker
	}
	// 空闲次数
	if consumerConf.FreeTimes <= 0 {
		consumerConf.FreeTimes = 60
	}

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
	fCNum := int(GetFreeCNum())

	if length >= conf.WaitNum && consumerLength < conf.MaxWorker { //消息积压大于阈值 启动新的协程
		add_num := conf.MaxWorker - consumerLength
		if add_num > conf.AddWorker {
			add_num = conf.AddWorker
		}
		startMultiConsumer(add_num)
		freeTimes = 0
	} else if length < 1 && consumerLength > conf.MinWorker && fCNum > 0 { //无积压消息 且有空闲协程 关闭协程
		// 空闲次数++
		freeTimes++
		// 空闲次数大于等于 阈值 关闭协程
		if freeTimes >= conf.FreeTimes {
			diff := consumerLength - conf.MinWorker
			if diff > conf.AddWorker {
				diff = conf.AddWorker
				// 如果diff大于等于2 则每次 conf.AddWorker的一半
				if diff >= 2 {
					diff = conf.AddWorker / 2
				}
			}
			if diff > fCNum {
				diff = fCNum
			}
			stopMultiConsumer(diff)
			freeTimes = 0
		}
	} else {
		// 清空空闲次数
		freeTimes = 0
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
	num := len(consumers)
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
	log.Println("关闭消费着 ", num)
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
