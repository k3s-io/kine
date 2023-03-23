package endpoint

import "github.com/AdamShannag/kine/pkg/drivers/oracle"

func OracleConnectionPoolConfig(cfg Config) oracle.ConnectionPoolConfig {
	return oracle.ConnectionPoolConfig{
		MaxIdle:     cfg.ConnectionPoolConfig.MaxIdle,
		MaxOpen:     cfg.ConnectionPoolConfig.MaxOpen,
		MaxLifetime: cfg.ConnectionPoolConfig.MaxLifetime,
	}
}
