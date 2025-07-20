package scheduler

import (
	"context"
	"sync"

	"github.com/AttackMAX/GeeClock/common/conf"
	"github.com/AttackMAX/GeeClock/pkg/log"
	service "github.com/AttackMAX/GeeClock/service/scheduler"
)

// 读取配置启动多个协程进行
type WorkerApp struct {
	sync.Once
	service workerService
	ctx     context.Context
	stop    func()
}

func NewWorkerApp(service *service.Worker) *WorkerApp {
	w := WorkerApp{
		service: service,
	}

	w.ctx, w.stop = context.WithCancel(context.Background())
	return &w
}

func (w *WorkerApp) Start() {
	w.Do(w.start)
}

func (w *WorkerApp) start() {
	log.InfoContext(w.ctx, "worker app is starting")
	go func() {
		if err := w.service.Start(w.ctx); err != nil {
			log.ErrorContextf(w.ctx, "worker start failed, err: %v", err)
		}
	}()
}

func (w *WorkerApp) Stop() {
	w.stop()
	log.WarnContext(w.ctx, "worker app is stopped")
}

type workerService interface {
	Start(context.Context) error
}

type confProvider interface {
	Get() *conf.SchedulerAppConf
}
