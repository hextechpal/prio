package config

import (
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Debug bool `envconfig:"PRIO_DEBUG"`

	Namespace string `envconfig:"PRIO_NAMESPACE"`

	Server struct {
		Host string `envconfig:"PRIO_SERVER_HOST"`
		Port int32  `envconfig:"PRIO_SERVER_PORT"`
	}

	DB struct {
		Host     string `envconfig:"PRIO_DB_HOST"`     // Only sql is supported for now
		Port     int32  `envconfig:"PRIO_DB_PORT"`     // Only sql is supported for now
		User     string `envconfig:"PRIO_DB_USER"`     // Only sql is supported for now
		Password string `envconfig:"PRIO_DB_PASSWORD"` // Only sql is supported for now
		Database string `envconfig:"PRIO_DB_DATABASE"` // Only sql is supported for now
	}

	Zk struct {
		Servers   []string `envconfig:"PRIO_ZK_SERVERS"`    // list of zookeeper servers
		TimeoutMs int32    `envconfig:"PRIO_ZK_TIMEOUT_MS"` // timeout in millisecond
	}
}

func Load() (*Config, error) {
	cfg := Config{}
	err := envconfig.Process("", &cfg)
	return &cfg, err
}
