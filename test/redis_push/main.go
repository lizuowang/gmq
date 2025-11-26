package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/lizuowang/gmq/redis_worker"
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
	conf := &redis_worker.RedisWorkerConf{
		RedisCli:  client,
		ListenKey: "test",
		DelayKey:  "test_delay",
		L:         L,
		Name:      "storeMq",
	}

	fmt.Println("开始推送消息")
	//休眠10秒
	// time.Sleep(1 * time.Second)

	// msgs := make([]string, 0)
	// for i := 0; i < 40; i++ {
	// 	msgs = append(msgs, "aaaa"+strconv.Itoa(i))
	// }
	// redis_worker.PushMsgListByConf(conf, msgs)

	for i := 0; i < 100; i++ {
		redis_worker.PushDelayMsgByConf(conf, "aaaa"+strconv.Itoa(i), time.Second*1)
	}

	len := redis_worker.GetMsgNumByConf(conf)
	fmt.Println("len", len)

	//监听ctrl+c
	// c := make(chan os.Signal, 1)
	// signal.Notify(c, os.Interrupt)
	// <-c

}

// 处理消息
func handleMsg(msg string) (newMsg string) {
	// time.Sleep(time.Millisecond * 2000)
	// fmt.Println(msg)
	return newMsg
}
