package task_worker

import "go.uber.org/zap"

type Worker struct {
	quitChan chan bool
	idx      int
	wm       *WorkerM // 消费者管理器
}

// 创建一个消费者
func NewWorker(idx int, wm *WorkerM) *Worker {
	return &Worker{
		quitChan: make(chan bool),
		idx:      idx,
		wm:       wm,
	}
}

// 启动消费者
func (w *Worker) Start() {
	defer func() {
		if r := recover(); r != nil {
			w.wm.Conf.L.Error(w.wm.GetLogMsg("Worker.Start error "), zap.Int("idx", w.idx), zap.Any("error", r))
		}
		w.wm.DeleteWorker(w)
	}()

	w.wm.Conf.L.Info(w.wm.GetLogMsg("Worker.Start start "), zap.Int("idx", w.idx))

	// 增加空闲协程数量
	w.wm.IncrFreeCNum()

	// 协程结束 减少空闲协程数量
	defer w.wm.DecrFreeCNum()

	for {
		select {
		case <-w.quitChan:
			return
		case msg, ok := <-w.wm.MsgChan:
			if !ok {
				return
			}

			// 减少空闲协程数量
			w.wm.DecrFreeCNum()
			newMsg := w.wm.Conf.Handler(msg)
			// 处理消息失败 重新投递
			if newMsg != "" {
				w.wm.Conf.FailPushMsh(newMsg)
			}

			// 增加空闲协程数量
			w.wm.IncrFreeCNum()
		}
	}
}

// 关闭一个消费者
func (w *Worker) Stop() {
	defer func() {
		if r := recover(); r != nil {
			w.wm.Conf.L.Error(w.wm.GetLogMsg("Worker.stop error "), zap.Int("idx", w.idx), zap.Any("error", r))
		}
	}()
	close(w.quitChan)
}
