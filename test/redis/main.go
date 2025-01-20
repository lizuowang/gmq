package main

import (
	"fmt"
	"os"
	"os/signal"
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
		MinWorker: 80,
		MaxWorker: 800,
		AddWorker: 5,
		WaitNum:   2,
		FreeTimes: 10,
		L:         L,
	}

	redis_mq.InitRedisMQ(conf, consumerConf)

	go func() {
		for {
			fmt.Println("空闲协程数量", consumer.GetFreeCNum())
			time.Sleep(1 * time.Second)
		}
	}()

	//休眠10秒
	time.Sleep(1 * time.Second)
	for i := 0; i < 10000; i++ {
		redis_mq.PushMsg("aaaa" + strconv.Itoa(i))
	}

	//监听 ctrl+c 信号
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	redis_mq.Stop()
	fmt.Println("数", redis_mq.GetChanMsgNum())

}

// 处理消息
func handleMsg(msg string) (newMsg string) {
	time.Sleep(time.Millisecond * 500)
	// fmt.Println(msg)
	return newMsg
}
