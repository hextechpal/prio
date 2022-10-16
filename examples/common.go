package examples

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/hextechpal/prio/commons"
	"github.com/hextechpal/prio/internal/config"
	"github.com/hextechpal/prio/internal/store/mysql"
	"github.com/hextechpal/prio/internal/worker"
)

const (
	user     = "root"
	password = "root"
	host     = "127.0.0.1"
	port     = 3306
	dbname   = "prio"

	zkHost = "127.0.0.1"
)

func setup(ctx context.Context, t *testing.T) (*worker.Worker, error) {
	t.Helper()
	c := &config.Config{
		Driver: "mysql",
		DSN:    fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, dbname),
	}
	namespace := fmt.Sprintf("ns_%d", rand.Intn(100))
	l := commons.NewLogger(ctx)
	s, err := mysql.NewStorage(c, l)
	if err != nil {
		t.Logf("failed to init storage: %s", err.Error())
		t.FailNow()
	}

	conn, _, err := zk.Connect([]string{zkHost}, 10*time.Second)
	return worker.NewWorker(namespace, s, conn)
}
