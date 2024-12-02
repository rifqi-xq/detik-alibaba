//go:generate goagen bootstrap -d analytic-collector/api/design

package api

import (
	"context"
	"net"
	"net/http"
	"time"

	"analytic-collector/api/app"
	"analytic-collector/api/controllers"
	"analytic-collector/services"

	"github.com/Sirupsen/logrus"
	"github.com/goadesign/goa"
	goalog15 "github.com/goadesign/goa/logging/log15"
	"github.com/goadesign/goa/middleware"
	"github.com/inconshreveable/log15"
	"github.com/tylerb/graceful"
)

// Run initializes and starts the API server for the analytic-collector service.
// It configures middleware, mounts controllers, and ensures graceful shutdown.
func Run(ctx context.Context, listener net.Listener, appService services.ServiceInterface, log log15.Logger) error {
	var err error

	// Create a new Goa service named "analytic-collector".
	service := goa.New("analytic-collector")

	// Middleware setup for request handling and error management.
	service.Use(middleware.RequestID())                 // Adds a unique request ID to each request for easier tracing.
	service.Use(middleware.LogRequest(true))            // Logs incoming requests with method and path.
	service.Use(middleware.ErrorHandler(service, true)) // Standard error handling to return appropriate HTTP responses.
	service.Use(middleware.Recover())                   // Prevents server crashes by recovering from panics.
	service.WithLogger(goalog15.New(log))               // Configures the logger for the service.

	// Mount the "collector" controller, responsible for main analytic collection endpoints.
	c1 := controllers.NewCollectorController(service, appService)
	app.MountCollectorController(service, c1)

	// Mount the "swagger" controller, serving API documentation.
	c2 := controllers.NewSwaggerController(service)
	app.MountSwaggerController(service, c2)

	// Mount the "version" controller, providing API versioning details.
	c3 := controllers.NewVersionController(service)
	app.MountVersionController(service, c3)

	// Setup a server with graceful shutdown capabilities.
	server := &graceful.Server{
		NoSignalHandling: true, // Disables automatic signal handling (e.g., SIGINT).
		Server: &http.Server{
			Handler: service.Mux, // Goa service's multiplexer handles all incoming requests.
		},
	}

	// Start the server in a separate goroutine to allow non-blocking control.
	c := make(chan error, 1)
	go func() {
		c <- server.Serve(listener) // Start listening for HTTP requests.
	}()

	// Handle graceful shutdown or server errors.
	select {
	case <-ctx.Done(): // Triggered when the context is canceled.
		// Stop workers if applicable (commented out as an example).
		// appService.StopWorkers()

		// Gracefully stop the server with a timeout.
		server.Stop(3 * time.Second)
		<-server.StopChan() // Wait until the server stops completely.

		// Drain any remaining error messages.
		<-c
	case err := <-c: // Handles errors from the server.
		if err != nil {
			logrus.Error(err.Error()) // Log the error for debugging.
		}
	}

	return err // Return any error encountered during server operation.
}
