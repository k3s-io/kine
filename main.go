package main

import (
	"context"
	"errors"
	"os"

	"github.com/k3s-io/kine/pkg/app"
	"github.com/sirupsen/logrus"
)

func main() {
	app := app.New()
	if err := app.Run(os.Args); err != nil {
		if !errors.Is(err, context.Canceled) {
			logrus.Fatal(err)
		}
	}
}
