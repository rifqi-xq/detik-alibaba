package main

import (
	"os"

	"github.com/inconshreveable/log15"
	"github.com/urfave/cli"
	"analytic-collector/command/run"
	"analytic-collector/version"
)

var logHandler log15.Handler

func main() {
	app := cli.NewApp()
	app.Name = "analytic-collector"
	app.Usage = "Analytic collector server component"
	app.Copyright = "(c) 2018 Detiknetwork"
	app.Version = version.Version + " (" + version.GitCommit + ")"
	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "debug, d",
			Usage: "Enable verbose logging",
		},
	}
	app.Before = func(c *cli.Context) error {
		f := log15.JsonFormat()
		if c.Bool("debug") {
			log15.Root().SetHandler(log15.CallerStackHandler("%+v", log15.StreamHandler(os.Stdout, f)))
		} else {
			log15.Root().SetHandler(log15.LvlFilterHandler(log15.LvlError, log15.CallerFileHandler(log15.StreamHandler(os.Stdout, f))))
		}

		return nil
	}
	app.Commands = []cli.Command{
		run.Command,
	}

	if err := app.Run(os.Args); err != nil {
		log15.Crit(err.Error())
	}
}
