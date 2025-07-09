package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/lizuowang/gmq/redis_worker"
	"github.com/lizuowang/gmq/task_worker"
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
		L:         L,
		Name:      "storeMq",
	}

	WMConf := &task_worker.WorkerMConf{
		Handler:   handleMsg,
		MinWorker: 80,
		MaxWorker: 800,
		AddWorker: 100,
		WaitNum:   2,
		FreeTimes: 10,
		L:         L,
		Name:      "storeMq",
	}

	redisWorker := redis_worker.NewRedisWorker(conf, WMConf)

	go func() {
		for {
			fmt.Println("空闲协程数量", redisWorker.WM.GetFreeCNum())
			fmt.Println("堆积消息数量", redisWorker.GetChanMsgNum())
			time.Sleep(1 * time.Second)
		}
	}()

	//休眠10秒
	time.Sleep(1 * time.Second)
	// for i := 0; i < 10000; i++ {
	// 	redisWorker.PushMsg("aaaa" + strconv.Itoa(i))
	// }

	//监听 ctrl+c 信号
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	redisWorker.Stop()
	fmt.Println("数", redisWorker.GetChanMsgNum())

}

// 处理消息
func handleMsg(msg string) (newMsg string) {
	time.Sleep(time.Millisecond * 500)
	// fmt.Println(msg)
	return newMsg
}
