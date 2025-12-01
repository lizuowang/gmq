package redis_worker

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/lizuowang/gmq/task_worker"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// redis 消费者管理器配置
type RedisWorkerConf struct {
	RedisCli      *redis.Client // redis 客户端
	ListenKey     string        // 监听的key
	DelayKey      string        // 延迟队列的key
	L             *zap.Logger   // 日志
	Name          string        // 名称
	DelayRunScore func() int64  // 延迟时间戳
}

// redis 消费者管理器
type RedisWorker struct {
	Conf           *RedisWorkerConf // 配置
	ctx            context.Context
	cancel         context.CancelFunc
	WM             *task_worker.WorkerM
	LuaPopNum      int // 批量拉取数量
	LuaDelayPopNum int // 延迟批量拉取数量
}

// 批量拉取消息
var luaBatchLpop = redis.NewScript(`
local key = KEYS[1]
local n = tonumber(ARGV[1])

local res = {}
for i = 1, n do
    local v = redis.call("LPOP", key)
    if not v then
        break
    end
    table.insert(res, v)
end

return res
`)

// 延迟队列转移
var luaDelayTransfer = redis.NewScript(`
local zkey = KEYS[1]
local lkey = KEYS[2]
local now = tonumber(ARGV[1])
local maxCount = tonumber(ARGV[2])

local msgs = redis.call("ZRANGEBYSCORE", zkey, "-inf", now, "LIMIT", 0, maxCount)
if #msgs == 0 then
    return 0
end

redis.call("ZREM", zkey, unpack(msgs))
redis.call("RPUSH", lkey, unpack(msgs))

return #msgs
`)

// 投递消息
func PushMsgByConf(conf *RedisWorkerConf, msg string) {
	conf.RedisCli.RPush(context.Background(), conf.ListenKey, msg)
}

// 批量投递消息
func PushMsgListByConf(conf *RedisWorkerConf, msgs []string) {
	conf.RedisCli.RPush(context.Background(), conf.ListenKey, msgs)
}

// 投递延迟消息
// delay 延迟时间
func PushDelayMsgByConf(conf *RedisWorkerConf, msg string, delay time.Duration) {
	if conf.DelayKey == "" {
		return
	}
	conf.RedisCli.ZAdd(context.Background(), conf.DelayKey, redis.Z{
		Score:  float64(time.Now().Add(delay).Unix()),
		Member: msg,
	})
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

// 根据时间戳获取延迟队列长度
func GetDelayMsgNumByConf(conf *RedisWorkerConf, score int64) int64 {
	if conf.DelayKey == "" {
		return 0
	}
	length, err := conf.RedisCli.ZCount(context.Background(), conf.DelayKey, "-inf", strconv.FormatInt(score, 10)).Result()
	if err != nil {
		conf.L.Error("redis_worker.GetDelayMsgNum error", zap.Error(err))
		return 0
	}
	return length
}

// new RedisWorker
func NewRedisWorker(RWConf *RedisWorkerConf, WMConf *task_worker.WorkerMConf) *RedisWorker {

	if RWConf.DelayRunScore == nil {
		RWConf.DelayRunScore = func() int64 {
			return time.Now().Unix()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	rw := &RedisWorker{
		Conf:   RWConf,
		ctx:    ctx,
		cancel: cancel,
	}

	WMConf.FailPushMsh = rw.PushMsg

	rw.WM = task_worker.NewWorkerM(WMConf)

	go rw.startRedisMQ()

	go rw.startDelayScheduler()

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

	luaPopNum := rw.LuaPopNum
	if luaPopNum <= 0 {
		luaPopNum = 50
	} else if luaPopNum > 100 {
		luaPopNum = 100
	}
	block := time.Second * 5

	for {
		select {
		case <-rw.ctx.Done():
			return
		default:
		}
		msg, err := rw.Conf.RedisCli.BLPop(rw.ctx, block, rw.Conf.ListenKey).Result()

		if err != nil {
			if err != redis.Nil {
				rw.Conf.L.Error(rw.GetLogMsg("redis_worker.StartRedisMQ error"), zap.Error(err))
				time.Sleep(time.Second * 1)
			}
		} else if msg != nil {
			// 如果消息长度小于2 则跳过
			if len(msg) < 2 {
				continue
			}
			rw.WM.PushMsg(msg[1])
			// 用 Lua 再批量弹出 luaPopNum 条
			more, err := luaBatchLpop.Run(
				rw.ctx,
				rw.Conf.RedisCli,
				[]string{rw.Conf.ListenKey},
				luaPopNum,
			).StringSlice()
			if err != nil && err != redis.Nil {
				rw.Conf.L.Error(rw.GetLogMsg("luaBatchLpop error"), zap.Error(err))
			} else {
				for _, m := range more {
					rw.WM.PushMsg(m)
				}
			}
		}

	}
}

// 启动延迟调度器
func (rw *RedisWorker) startDelayScheduler() {

	if rw.Conf.DelayKey == "" {
		return
	}

	defer func() {
		rw.Conf.L.Info(rw.GetLogMsg("RedisWorker.startDelayScheduler stop"))
		r := recover()
		if r != nil {
			rw.Conf.L.Error(rw.GetLogMsg("RedisWorker.startDelayScheduler stop error "), zap.Any("error", r))
			// 重新启动
			go rw.startDelayScheduler()
		}
	}()

	rw.Conf.L.Info(rw.GetLogMsg("RedisWorker.startDelayScheduler start"))
	luaDelayPopNum := rw.LuaDelayPopNum
	if luaDelayPopNum <= 0 {
		luaDelayPopNum = 50
	} else if luaDelayPopNum > 100 {
		luaDelayPopNum = 100
	}

	for {
		select {
		case <-rw.ctx.Done():
			return
		default:
		}

		runScore := rw.Conf.DelayRunScore() // 你用秒就这里传秒
		n, err := luaDelayTransfer.Run(
			rw.ctx,
			rw.Conf.RedisCli,
			[]string{rw.Conf.DelayKey, rw.Conf.ListenKey},
			runScore,
			luaDelayPopNum,
		).Int()
		if err != nil && err != redis.Nil {
			rw.Conf.L.Error(rw.GetLogMsg("RedisWorker.startDelayScheduler error"), zap.Error(err))
			continue
		}
		if n >= luaDelayPopNum {
			// 说明还有可能有大量积压，立刻再来一轮，不 sleep
			continue
		}

		// 本轮取不到满批次，说明基本快空了，休眠 1 秒再检查
		time.Sleep(time.Second)
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

// 根据时间戳获取延迟队列长度
func (rw *RedisWorker) GetDelayMsgNum(score int64) int64 {
	if rw.Conf.DelayKey == "" {
		return 0
	}
	length, err := rw.Conf.RedisCli.ZCount(context.Background(), rw.Conf.DelayKey, "-inf", strconv.FormatInt(score, 10)).Result()
	if err != nil {
		rw.Conf.L.Error(rw.GetLogMsg("redis_worker.GetDelayMsgNum error"), zap.Error(err))
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
