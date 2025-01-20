package main

import (
	"strconv"
	"time"

	"github.com/lizuowang/gmq/consumer"
	"github.com/lizuowang/gmq/redis_mq"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func main() {

	redisConf := &redis.Options{
		Addr:         "192.168.1.191:1079",
		Password:     "123456",
		DB:           0,
		PoolSize:     256,
		MinIdleConns: 32,
	}
	client := redis.NewClient(redisConf)

	L := zap.NewExample()
	conf := &redis_mq.RedisMQConf{
		RedisCli:  client,
		ListenKey: "test",
		L:         L,
	}

	consumerConf := &consumer.ConsumerConf{
		Handler:   handleMsg,
		MinWorker: 1,
		MaxWorker: 1,
		AddWorker: 1,
		WaitNum:   2,
		FreeTimes: 10,
		L:         L,
	}

	redis_mq.InitRedisMQ(conf, consumerConf)

	//休眠10秒
	time.Sleep(1 * time.Second)
	for i := 0; i < 10000; i++ {
		redis_mq.PushMsg("aaaa" + strconv.Itoa(i))
	}

	redis_mq.Stop()

}

// 处理消息
func handleMsg(msg string) (newMsg string) {
	time.Sleep(time.Millisecond * 500)
	// fmt.Println(msg)
	return newMsg
}
