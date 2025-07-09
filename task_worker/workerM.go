package task_worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

var (
	defaultChanSize = 200 // 默认通道大小
)

// 消费者管理器配置
type WorkerMConf struct {
	Handler     func(msg string) (newMsg string) // 处理消息的函数
	FailPushMsh func(msg string)                 // 失败消息重新投递
	MinWorker   int                              // 最小worker数量
	MaxWorker   int                              // 最大worker数量
	AddWorker   int                              // 每次启动的协程数量
	WaitNum     int                              // 等待消息数量 大于这个数量 启动协程
	FreeTimes   int                              // 空闲次数 大于这个数量 关闭协程
	L           *zap.Logger                      // 日志
	Name        string                           // 日志名称
	ChanSize    int                              // 通道大小
}

// 消费者管理器
type WorkerM struct {
	Conf      *WorkerMConf       // 配置
	MsgChan   chan string        // 消息通道
	ctx       context.Context    // 上下文
	cancel    context.CancelFunc // 取消函数
	workers   map[*Worker]bool   // 消费者
	mu        sync.Mutex         // 互斥锁
	freeTimes int                // 空闲次数
	freeCNum  int32              // 空闲协程数量
}

// 实例化一个消费者管理器
func NewWorkerM(conf *WorkerMConf) *WorkerM {

	if conf == nil {
		log.Fatalf("WorkerM 配置错误")
	}
	if conf.Handler == nil {
		log.Fatalf("WorkerM WorkerMConf.Handler 不能为空")
	}
	if conf.MinWorker <= 0 {
		log.Fatalf("WorkerM WorkerMConf.MinWorker 不能小于等于0")
	}
	if conf.MaxWorker <= 0 {
		log.Fatalf("WorkerM WorkerMConf.MaxWorker 不能为空")
	}
	if conf.L == nil {
		log.Fatalf("WorkerM WorkerMConf.L 不能为空")
	}
	// 等待消息数量
	if conf.WaitNum <= 0 {
		conf.WaitNum = 2
	}
	// 每次启动的协程数量
	if conf.AddWorker <= 0 {
		conf.AddWorker = conf.MinWorker
	}
	// 空闲次数
	if conf.FreeTimes <= 0 {
		conf.FreeTimes = 60
	}

	// 通道大小
	if conf.ChanSize <= 0 {
		conf.ChanSize = defaultChanSize
	}

	ctx, cancel := context.WithCancel(context.Background())
	workerM := &WorkerM{
		Conf:      conf,
		MsgChan:   make(chan string, conf.ChanSize),
		ctx:       ctx,
		cancel:    cancel,
		workers:   make(map[*Worker]bool),
		mu:        sync.Mutex{},
		freeTimes: 0,
		freeCNum:  0,
	}
	go workerM.start()
	return workerM
}

// 增加空闲协程数量
func (wm *WorkerM) IncrFreeCNum() {
	atomic.AddInt32(&wm.freeCNum, 1)
}

// 减少空闲协程数量
func (wm *WorkerM) DecrFreeCNum() {
	atomic.AddInt32(&wm.freeCNum, -1)
}

// 获取空闲协程数量
func (wm *WorkerM) GetFreeCNum() int32 {
	return atomic.LoadInt32(&wm.freeCNum)
}

// 获取总协程数
func (wm *WorkerM) GetTotalCNum() int {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	return len(wm.workers)
}

// 获取日志消息
func (wm *WorkerM) GetLogMsg(msg string) string {
	return fmt.Sprintf("%s.%s", wm.Conf.Name, msg)
}

// 启动消费者
func (wm *WorkerM) start() {
	defer func() { //异常退出是 重新启动
		if r := recover(); r != nil {
			wm.Conf.L.Error(wm.GetLogMsg("WorkerM.start stop error "), zap.Any("error", r))
			// 重新启动
			go wm.start()
		}
	}()

	for {
		select {
		case <-wm.ctx.Done():
			return
		default:
			sleepTime := wm.manageWorkerM()
			time.Sleep(sleepTime) // 每1秒检查一次
		}
	}
}

// 管理消费者
func (wm *WorkerM) manageWorkerM() time.Duration {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	sleepTime := time.Second * 1

	workerNum := wm.GetWorkerNum()
	if workerNum < wm.Conf.MinWorker { //默认启动一个协程
		diff := wm.Conf.MinWorker - workerNum
		wm.startMultiWorker(diff)
		return sleepTime
	}

	length := wm.GetChanMsgLen()
	fCNum := int(wm.freeCNum)

	if length >= wm.Conf.WaitNum && workerNum < wm.Conf.MaxWorker { //消息积压大于阈值 启动新的协程
		add_num := wm.Conf.MaxWorker - workerNum
		if add_num > wm.Conf.AddWorker {
			add_num = wm.Conf.AddWorker
		}
		wm.startMultiWorker(add_num)
		wm.freeTimes = 0
	} else if length < 1 && workerNum > wm.Conf.MinWorker && wm.freeCNum > 0 { //无积压消息 且有空闲协程 关闭协程
		// 空闲次数++
		wm.freeTimes++
		// 空闲次数大于等于 阈值 关闭协程
		if wm.freeTimes >= wm.Conf.FreeTimes {
			diff := workerNum - wm.Conf.MinWorker
			if diff > wm.Conf.AddWorker {
				diff = wm.Conf.AddWorker
				// 如果diff大于等于2 则每次 conf.AddWorker的一半
				if diff >= 2 {
					diff = wm.Conf.AddWorker / 2
				}
			}
			if diff > fCNum {
				diff = fCNum
			}
			wm.stopMultiWorker(diff)
			wm.freeTimes = 0
		}
	} else {
		// 清空空闲次数
		wm.freeTimes = 0
	}

	return sleepTime
}

// 启动多个消费者
func (wm *WorkerM) startMultiWorker(num int) {
	for i := 0; i < num; i++ {
		worker := NewWorker(wm.GetWorkerNum(), wm)
		wm.workers[worker] = true
		go worker.Start()
	}
}

// 关闭多个消费者
func (wm *WorkerM) stopMultiWorker(num int) {
	i := 0
	for worker := range wm.workers {
		worker.Stop()
		i++
		if i >= num {
			break
		}
	}
}

// 投递消息
func (wm *WorkerM) PushMsg(msg string) {
	wm.MsgChan <- msg
}

// 获取消费者数量
func (wm *WorkerM) GetWorkerNum() int {

	return len(wm.workers)
}

// 获取chan消息数量
func (wm *WorkerM) GetChanMsgLen() int {
	return len(wm.MsgChan)
}

// 删除消费者
func (wm *WorkerM) DeleteWorker(c *Worker) {
	wm.mu.Lock()
	delete(wm.workers, c)
	num := len(wm.workers)
	wm.mu.Unlock()
	wm.Conf.L.Info(wm.GetLogMsg("WorkerM.DeleteWorker stop "), zap.Int("idx", c.idx), zap.Int("num", num))
}

// 关闭所有消费者
func (wm *WorkerM) stopAllWorker() {
	wm.mu.Lock()
	num := len(wm.workers)
	for worker := range wm.workers {
		worker.Stop()
	}
	wm.mu.Unlock()

	//睡100毫秒
	time.Sleep(time.Millisecond * 100)
	for {
		wm.mu.Lock()
		len := len(wm.workers)
		wm.mu.Unlock()
		if len == 0 {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	log.Println("关闭消费着 ", num)
}

// 停止消费者
func (wm *WorkerM) Stop(rePushMsg func(msg string)) {
	defer func() {
		wm.Conf.L.Info(wm.GetLogMsg("WorkerM.Stop stop all Worker"))
		if r := recover(); r != nil {
			wm.Conf.L.Error(wm.GetLogMsg("WorkerM.stop error "), zap.Any("error", r))
		}
	}()

	wm.cancel()

	wm.stopAllWorker()

	// 重新投递消息
	for msg := range wm.MsgChan {
		rePushMsg(msg)
	}

}
