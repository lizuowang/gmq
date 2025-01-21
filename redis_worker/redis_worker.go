package redis_worker

import (
	"context"
	"fmt"
	"time"

	"github.com/lizuowang/gmq/task_worker"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// redis 消费者管理器配置
type RedisWorkerConf struct {
	RedisCli  *redis.Client // redis 客户端
	ListenKey string        // 监听的key
	L         *zap.Logger   // 日志
	Name      string        // 名称
}

// redis 消费者管理器
type RedisWorker struct {
	Conf   *RedisWorkerConf // 配置
	ctx    context.Context
	cancel context.CancelFunc
	WM     *task_worker.WorkerM
}

// 投递消息
func PushMsgByConf(conf *RedisWorkerConf, msg string) {
	conf.RedisCli.RPush(context.Background(), conf.ListenKey, msg)
}

// 获取消息队列长度
func GetMsgNumByConf(conf *RedisWorkerConf) int64 {
	length, err := conf.RedisCli.LLen(context.Background(), conf.ListenKey).Result()
	if err != nil {
		conf.L.Error("redis_worker.GetMsgListLen error", zap.Error(err))
		return 0
	}
	return length
}

// new RedisWorker
func NewRedisWorker(RWConf *RedisWorkerConf, WMConf *task_worker.WorkerMConf) *RedisWorker {
	ctx, cancel := context.WithCancel(context.Background())
	rw := &RedisWorker{
		Conf:   RWConf,
		ctx:    ctx,
		cancel: cancel,
	}

	WMConf.FailPushMsh = rw.PushMsg

	rw.WM = task_worker.NewWorkerM(WMConf)

	go rw.startRedisMQ()

	return rw
}

// 获取日志消息
func (rw *RedisWorker) GetLogMsg(msg string) string {
	return fmt.Sprintf("%s.%s", rw.Conf.Name, msg)
}

// 启动redis mq
func (rw *RedisWorker) startRedisMQ() {

	defer func() {
		rw.Conf.L.Info(rw.GetLogMsg("RedisWorker.startRedisMQ stop"))
		r := recover()
		if r != nil {
			rw.Conf.L.Error(rw.GetLogMsg("RedisWorker.startRedisMQ stop error "), zap.Any("error", r))
			// 重新启动
			go rw.startRedisMQ()
		} else {
			close(rw.WM.MsgChan)
		}
	}()

	rw.Conf.L.Info(rw.GetLogMsg("RedisWorker.startRedisMQ start"))

	for {
		select {
		case <-rw.ctx.Done():
			return
		default:
			msg, err := rw.Conf.RedisCli.BLPop(rw.ctx, time.Second*5, rw.Conf.ListenKey).Result()
			if err != nil {
				if err != redis.Nil {
					rw.Conf.L.Error(rw.GetLogMsg("redis_worker.StartRedisMQ error"), zap.Error(err))
					time.Sleep(time.Second * 1)
				}
			} else if msg != nil {
				rw.WM.PushMsg(msg[1])
			}
		}
	}
}

// 重新投递消息
func (rw *RedisWorker) RePushMsg(msg string) {
	rw.Conf.L.Error(rw.GetLogMsg("RedisWorker.RePushMsg"), zap.String("msg", msg))
	rw.Conf.RedisCli.LPush(context.Background(), rw.Conf.ListenKey, msg)
}

// 投递消息
func (rw *RedisWorker) PushMsg(msg string) {
	PushMsgByConf(rw.Conf, msg)
}

// 获取消息队列长度
func (rw *RedisWorker) GetMsgNum() int64 {
	length, err := rw.Conf.RedisCli.LLen(context.Background(), rw.Conf.ListenKey).Result()
	if err != nil {
		rw.Conf.L.Error(rw.GetLogMsg("redis_worker.GetMsgListLen error"), zap.Error(err))
		return 0
	}
	return length
}

// 获取chan消息数量
func (rw *RedisWorker) GetChanMsgNum() int {
	return rw.WM.GetChanMsgLen()
}

// 停止redis mq
func (rw *RedisWorker) Stop() {
	rw.cancel()
	rw.WM.Stop(rw.RePushMsg)
}
