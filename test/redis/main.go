package main

import (
	"fmt"
	"time"

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

	conf := &redis_mq.RedisMQConf{
		RedisCli:  client,
		ListenKey: "test",
		Handler:   handleMsg,
		MinWorker: 80,
		MaxWorker: 800,
		L:         zap.NewExample(),
	}

	redis_mq.InitRedisMQ(conf)
	//休眠10秒
	time.Sleep(1 * time.Second)
	// for i := 0; i < 10000; i++ {
	// 	redis_mq.PushMsg("aaaa" + strconv.Itoa(i))
	// }
	redis_mq.Stop()
	fmt.Println("数", redis_mq.GetChanMsgNum())

}

// 处理消息
func handleMsg(msg string) (newMsg string) {
	// time.Sleep(time.Millisecond * 500)
	fmt.Println(msg)
	return newMsg
}
