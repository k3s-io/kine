package endpoint

import (
	// Import all the default drivers
	_ "github.com/k3s-io/kine/pkg/drivers/mongodb"
	_ "github.com/k3s-io/kine/pkg/drivers/mysql"
	_ "github.com/k3s-io/kine/pkg/drivers/nats"
	_ "github.com/k3s-io/kine/pkg/drivers/pgsql"
	_ "github.com/k3s-io/kine/pkg/drivers/remote"
	_ "github.com/k3s-io/kine/pkg/drivers/sqlite"
)
