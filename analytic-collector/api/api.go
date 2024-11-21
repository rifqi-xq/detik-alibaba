//go:generate goagen bootstrap -d analytic-collector/api/design

package api

import (
	"net"
	"net/http"
	"time"

	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	"github.com/goadesign/goa"
	"github.com/goadesign/goa/logging/log15"
	"github.com/goadesign/goa/middleware"
	"github.com/inconshreveable/log15"
	"github.com/tylerb/graceful"
	"analytic-collector/api/app"
	"analytic-collector/api/controllers"
	"analytic-collector/services"
)

func Run(ctx context.Context, listener net.Listener, appService services.ServiceInterface, log log15.Logger) error {
	var err error

	// Create service
	service := goa.New("analytic-collector")

	// Mount middleware
	service.Use(middleware.RequestID())
	service.Use(middleware.LogRequest(true))
	service.Use(middleware.ErrorHandler(service, true))
	service.Use(middleware.Recover())
	service.WithLogger(goalog15.New(log))

	// Mount "collector" controller
	c1 := controllers.NewCollectorController(service, appService)
	app.MountCollectorController(service, c1)
	// Mount "swagger" controller
	c2 := controllers.NewSwaggerController(service)
	app.MountSwaggerController(service, c2)
	// Mount "version" controller
	c3 := controllers.NewVersionController(service)
	app.MountVersionController(service, c3)

	// Setup graceful server
	server := &graceful.Server{
		NoSignalHandling: true,
		Server: &http.Server{
			Handler: service.Mux,
		},
	}

	c := make(chan error, 1)
	go func() {
		c <- server.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		// stoping all workers
		appService.StopWorkers()

		server.Stop(time.Duration(3) * time.Second)
		<-server.StopChan()
		// draining the channel
		<-c
	case err := <-c:
		if err != nil {
			logrus.Error(err.Error())
		}
	}

	return err
}
