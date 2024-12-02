package run

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"

	"golang.org/x/net/context"

	"kafka-go/services"

	"github.com/inconshreveable/log15"
	"github.com/urfave/cli"
)

// Command defines the CLI command for running the analytic-collector service
var Command = cli.Command{
	Name:  "run",
	Usage: "Run the service",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:   "socket",
			Usage:  "REST API `socket` either as '[tcp://]<address>:<port>' or 'unix://<path>' string",
			EnvVar: "ANALYTIC_COLLECTOR_SOCKET", // Allows configuration via environment variable
			Value:  "tcp://127.0.0.1:8091",      // Default socket address
		},
		cli.StringFlag{
			Name:   "gcp-project-id",
			Usage:  "Google Cloud Project Id",
			EnvVar: "ANALYTIC_COLLECTOR_GCP_PROJECT_ID", // Allows configuration via environment variable
			Value:  "detikcom-179007",
		},
		cli.StringFlag{
			Name:   "gcp-credentials-file",
			Usage:  "GCP Service Account file path",
			EnvVar: "GOOGLE_APPLICATION_CREDENTIALS", // Allows configuration via environment variable
			Value:  "",
		},
		cli.StringFlag{
			Name:   "pubsub-topic-get-id",
			Usage:  "PubSub Topic GET Id",
			EnvVar: "ANALYTIC_COLLECTOR_GCP_TOPIC_GET_ID", // Allows configuration via environment variable
			Value:  "demo",
		},
		cli.StringFlag{
			Name:   "pubsub-topic-post-id",
			Usage:  "PubSub Topic POST Id",
			EnvVar: "ANALYTIC_COLLECTOR_GCP_TOPIC_POST_ID", // Allows configuration via environment variable
			Value:  "demo",
		},
		cli.IntFlag{
			Name:   "collector-worker",
			Usage:  "Collector Maximum Worker",
			EnvVar: "ANALYTIC_COLLECTOR_PARSER_MAX_WORKER", // Allows configuration via environment variable
			Value:  100,                                    // Default number of workers
		},
	},
	Action: func(c *cli.Context) error {
		log := log15.New("module", "analytic-collector")

		var err error
		var listener net.Listener

		// Determine socket type (Unix or TCP) and create a listener
		socket := c.String("socket")
		if strings.HasPrefix(socket, "unix://") {
			// Handle Unix socket: cleanup existing file, then create new
			f := strings.TrimPrefix(socket, "unix://")
			if _, err := os.Stat(f); err == nil {
				err = os.Remove(f)
				if err != nil {
					return err
				}
			}
			// Listen on Unix socket and set appropriate permissions
			if listener, err = net.Listen("unix", f); err == nil {
				err = os.Chmod(f, 0770)
			}
		} else {
			// Handle TCP socket
			if strings.HasPrefix(socket, "tcp://") {
				socket = strings.TrimPrefix(socket, "tcp://")
			}
			listener, err = net.Listen("tcp", socket)
		}
		if err != nil {
			return err // Return error if listener setup fails
		}

		// Handle graceful shutdown via OS interrupt signals
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			s := <-sig
			fmt.Println()
			log.Info(fmt.Sprintf("signal %s received", s.String()))
			cancel() // Cancel context on signal
		}()

		// Ensure GCP credentials environment variable is set
		if c.String("gcp-credentials-file") != "" {
			os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", c.String("gcp-credentials-file"))
		}

		// Validate required PubSub topic IDs
		if c.String("pubsub-topic-get-id") == "" || c.String("pubsub-topic-post-id") == "" {
			return errors.New("either pubsub-topic-get-id or pubsub-topic-post-id are empty")
		}

		// Initialize and configure the application service
		appService, err := services.NewService(
			ctx,
			c.Int("collector-worker"),
			c.String("gcp-project-id"),
			c.String("pubsub-topic-get-id"),
			c.String("pubsub-topic-post-id"),
		)
		if err != nil {
			log.Error(err.Error())
			return err // Return error if service initialization fails
		}

		// Start worker processes
		appService.StartWorkers()

		// Start the API server with the configured service
		return api.Run(ctx, listener, appService, log)
	},
}
