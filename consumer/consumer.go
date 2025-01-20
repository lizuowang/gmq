package consumer

import (
	"go.uber.org/zap"
)

type Consumer struct {
	quitChan chan bool
	idx      int
}

// 实例化一个消费者
func NewConsumer(conNum int) *Consumer {
	return &Consumer{
		quitChan: make(chan bool),
		idx:      conNum,
	}
}

// 启动消费者
func (c *Consumer) Start() {
	defer func() {
		deleteConsumer(c)
	}()

	conf.L.Info("Consumer.consumeMsg start ", zap.Int("idx", c.idx))

	// 增加空闲协程数量
	IncrFreeCNum()

	// 协程结束 减少空闲协程数量
	defer DecrFreeCNum()

	for {
		select {
		case <-c.quitChan:
			return
		case msg := <-MsgChan:
			// 减少空闲协程数量
			DecrFreeCNum()
			newMsg := conf.Handler(msg)
			// 处理消息失败 重新投递
			if newMsg != "" {
				conf.FailPushMsh(newMsg)
			}

			// 增加空闲协程数量
			IncrFreeCNum()
		}
	}
}

// 关闭一个消费者
func (c *Consumer) Stop() {
	defer func() {
		if r := recover(); r != nil {
			conf.L.Error("msg.Consumer.stop error ", zap.Int("idx", c.idx), zap.Any("error", r))
		}
	}()
	close(c.quitChan)
}
