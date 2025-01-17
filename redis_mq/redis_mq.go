package redis_mq

import (
	"context"
	"time"

	"github.com/lizuowang/gmq/consumer"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type RedisMQConf struct {
	RedisCli  *redis.Client                    // redis 客户端
	ListenKey string                           // 监听的key
	Handler   func(msg string) (newMsg string) // 处理消息的函数
	MinWorker int                              // 最小worker数量
	MaxWorker int                              // 最大worker数量
	L         *zap.Logger                      // 日志
}

var (
	conf   *RedisMQConf
	ctx    context.Context
	cancel context.CancelFunc
)

// 初始化redis mq
func InitRedisMQ(MQConf *RedisMQConf) {
	conf = MQConf

	consumerConf := &consumer.ConsumerConf{
		Handler:     conf.Handler,
		FailPushMsh: PushMsg,
		MinWorker:   conf.MinWorker,
		MaxWorker:   conf.MaxWorker,
		L:           conf.L,
	}

	consumer.InitConsumer(consumerConf)
	ctx, cancel = context.WithCancel(context.Background())
	go startRedisMQ()
}

// 启动redis mq
func startRedisMQ() {

	defer func() {
		conf.L.Info("redis_mq.startRedisMQ stop")
		if r := recover(); r != nil {
			conf.L.Error("redis_mq.startRedisMQ stop error ", zap.Any("error", r))
			// 重新启动
			go startRedisMQ()
		}
	}()

	conf.L.Info("redis_mq.startRedisMQ start")

	defer close(consumer.MsgChan)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := conf.RedisCli.BLPop(ctx, time.Second*5, conf.ListenKey).Result()
			if err != nil {
				if err != redis.Nil {
					conf.L.Error("by_redis.StartRedisMQ error", zap.Error(err))
					time.Sleep(time.Second * 1)
				}
			} else if msg != nil {
				consumer.PushMsg(msg[1])
			}
		}
	}
}

// 重新投递消息
func RePushMsg(msg string) {
	conf.L.Error("redis_mq.RePushMsg", zap.String("msg", msg))
	conf.RedisCli.LPush(context.Background(), conf.ListenKey, msg)
}

// 投递消息
func PushMsg(msg string) {
	conf.RedisCli.RPush(context.Background(), conf.ListenKey, msg)
}

// 获取消息队列长度
func GetMsgListLen() int64 {
	length, err := conf.RedisCli.LLen(context.Background(), conf.ListenKey).Result()
	if err != nil {
		conf.L.Error("by_redis.GetMsgListLen error", zap.Error(err))
		return 0
	}
	return length
}

// 获取chan消息数量
func GetChanMsgNum() int {
	return len(consumer.MsgChan)
}

// 停止redis mq
func Stop() {
	cancel()
	consumer.Stop(RePushMsg)
}
