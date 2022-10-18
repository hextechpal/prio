package config

import (
	"fmt"
	"math/rand"
	"time"

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
		Driver string `envconfig:"PRIO_DB_DRIVER"` // Only sql is supported for now
		DSN    string `envconfig:"PRIO_DB_DSN"`    // Only sql is supported for now
	}

	Zk struct {
		Servers   []string `envconfig:"PRIO_ZK_SERVERS"`    // list of zookeeper servers
		TimeoutMs int32    `envconfig:"PRIO_ZK_TIMEOUT_MS"` // timeout in mllisecond
	}
}

func Load() (*Config, error) {
	rand.Seed(time.Now().UnixMilli())
	cfg := Config{}
	err := envconfig.Process("", &cfg)
	cfg.Namespace = fmt.Sprintf("%s%d", cfg.Namespace, rand.Intn(1000))
	return &cfg, err
}
