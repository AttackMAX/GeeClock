package app

import (
	"go.uber.org/dig"

	"github.com/AttackMAX/GeeClock/app/migrator"
	"github.com/AttackMAX/GeeClock/app/monitor"
	"github.com/AttackMAX/GeeClock/app/scheduler"
	"github.com/AttackMAX/GeeClock/app/webserver"
	"github.com/AttackMAX/GeeClock/common/conf"
	taskdao "github.com/AttackMAX/GeeClock/dao/task"
	timerdao "github.com/AttackMAX/GeeClock/dao/timer"
	"github.com/AttackMAX/GeeClock/pkg/bloom"
	"github.com/AttackMAX/GeeClock/pkg/cron"
	"github.com/AttackMAX/GeeClock/pkg/hash"
	"github.com/AttackMAX/GeeClock/pkg/mysql"
	"github.com/AttackMAX/GeeClock/pkg/promethus"
	"github.com/AttackMAX/GeeClock/pkg/redis"
	"github.com/AttackMAX/GeeClock/pkg/xhttp"
	executorservice "github.com/AttackMAX/GeeClock/service/executor"
	migratorservice "github.com/AttackMAX/GeeClock/service/migrator"
	monitorservice "github.com/AttackMAX/GeeClock/service/monitor"
	schedulerservice "github.com/AttackMAX/GeeClock/service/scheduler"
	triggerservice "github.com/AttackMAX/GeeClock/service/trigger"
	webservice "github.com/AttackMAX/GeeClock/service/webserver"
)

var (
	container *dig.Container
)

func init() {
	container = dig.New()

	provideConfig(container)
	providePKG(container)
	provideDAO(container)
	provideService(container)
	provideApp(container)
}

func provideConfig(c *dig.Container) {
	c.Provide(conf.DefaultMysqlConfProvider)
	c.Provide(conf.DefaultSchedulerAppConfProvider)
	c.Provide(conf.DefaultTriggerAppConfProvider)
	c.Provide(conf.DefaultWebServerAppConfProvider)
	c.Provide(conf.DefaultRedisConfigProvider)
	c.Provide(conf.DefaultMigratorAppConfProvider)
}

func providePKG(c *dig.Container) {
	c.Provide(bloom.NewFilter)
	c.Provide(hash.NewMurmur3Encryptor)
	c.Provide(hash.NewSHA1Encryptor)
	c.Provide(redis.GetClient)
	c.Provide(mysql.GetClient)
	c.Provide(cron.NewCronParser)
	c.Provide(xhttp.NewJSONClient)
	c.Provide(promethus.GetReporter)
}

func provideDAO(c *dig.Container) {
	c.Provide(timerdao.NewTimerDAO)
	c.Provide(taskdao.NewTaskDAO)
	c.Provide(taskdao.NewTaskCache)
}

func provideService(c *dig.Container) {
	c.Provide(migratorservice.NewWorker)
	c.Provide(migratorservice.NewWorker)
	c.Provide(webservice.NewTaskService)
	c.Provide(webservice.NewTimerService)
	c.Provide(executorservice.NewTimerService)
	c.Provide(executorservice.NewWorker)
	c.Provide(triggerservice.NewWorker)
	c.Provide(triggerservice.NewTaskService)
	c.Provide(schedulerservice.NewWorker)
	c.Provide(monitorservice.NewWorker)
}

func provideApp(c *dig.Container) {
	c.Provide(migrator.NewMigratorApp)
	c.Provide(webserver.NewTaskApp)
	c.Provide(webserver.NewTimerApp)
	c.Provide(webserver.NewServer)
	c.Provide(scheduler.NewWorkerApp)
	c.Provide(monitor.NewMonitorApp)
}

func GetSchedulerApp() *scheduler.WorkerApp {
	var schedulerApp *scheduler.WorkerApp
	if err := container.Invoke(func(_s *scheduler.WorkerApp) {
		schedulerApp = _s
	}); err != nil {
		panic(err)
	}
	return schedulerApp
}

func GetWebServer() *webserver.Server {
	var server *webserver.Server
	if err := container.Invoke(func(_s *webserver.Server) {
		server = _s
	}); err != nil {
		panic(err)
	}
	return server
}

func GetMigratorApp() *migrator.MigratorApp {
	var migratorApp *migrator.MigratorApp
	if err := container.Invoke(func(_m *migrator.MigratorApp) {
		migratorApp = _m
	}); err != nil {
		panic(err)
	}
	return migratorApp
}

func GetMonitorApp() *monitor.MonitorApp {
	var monitorApp *monitor.MonitorApp
	if err := container.Invoke(func(_m *monitor.MonitorApp) {
		monitorApp = _m
	}); err != nil {
		panic(err)
	}
	return monitorApp
}
