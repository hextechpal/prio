package cmd

import (
	"context"
	"fmt"
	"github.com/hextechpal/prio/app/internal"
	"github.com/hextechpal/prio/app/internal/config"
	"github.com/hextechpal/prio/app/internal/handler"
	"github.com/hextechpal/prio/core"
	"github.com/hextechpal/prio/core/api"
	"github.com/hextechpal/prio/core/commons"
	"github.com/hextechpal/prio/engine/mysql"
	"github.com/joho/godotenv"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/labstack/gommon/log"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"net/http"
	"os"
	"os/signal"
	"time"
)

const envarg = "envfile"

// workerCmd represents the worker command
var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Starts a prio worker config",
	Long: `It starts a prio worker with the specified configuration 
It adds a worker to the worker pool specified by the name space
The topics are load balanced all the workers`,

	Run: func(cmd *cobra.Command, args []string) {
		path, _ := cmd.Flags().GetString(envarg)
		err := godotenv.Load(path)
		if err != nil {
			panic("error parsing config file")
		}

		c, err := config.Load()
		if err != nil {
			panic("error loading config")
		}
		setupServer(c)
	},
}

func setupServer(c *config.Config) {
	id := commons.GenerateUuid()
	ctx, cancel := context.WithCancel(context.Background())

	logger := initLogger(id, c)
	w, err := initWorker(id, c, logger)
	if err != nil {
		panic(err)
	}

	r := initRouter(c.Debug)
	g := r.Group("v1")

	h, err := handler.NewHandler(ctx, w, logger)
	if err != nil {
		panic(err)
	}
	h.Register(g)

	startServer(ctx, r, c, logger, cancel)
}

func startServer(ctx context.Context, r *echo.Echo, c *config.Config, logger commons.Logger, cancel context.CancelFunc) {
	// Start server
	go func() {
		err := r.Start(fmt.Sprintf("%s:%d", c.Server.Host, c.Server.Port))
		if err != nil && err != http.ErrServerClosed {
			cancel()
			logger.Fatal("shutting down the server, error=%v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	cancel()
	if err := r.Shutdown(ctx); err != nil {
		logger.Fatal("error=%v", err)
	}
}

func initRouter(debug bool) *echo.Echo {
	e := echo.New()
	lvl := log.INFO
	if debug {
		lvl = log.DEBUG
	}

	e.Logger.SetLevel(lvl)
	e.Pre(middleware.RemoveTrailingSlash())
	e.Use(middleware.Logger())
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowHeaders: []string{echo.HeaderOrigin, echo.HeaderContentType, echo.HeaderAccept, echo.HeaderAuthorization},
		AllowMethods: []string{echo.GET, echo.HEAD, echo.PUT, echo.PATCH, echo.POST, echo.DELETE},
	}))
	return e
}

func initWorker(id string, c *config.Config, logger commons.Logger) (*core.Worker, error) {
	engine, err := initEngine(c)
	if err != nil {
		return nil, err
	}
	timeout := time.Duration(c.Zk.TimeoutMs) * time.Millisecond
	w := core.NewWorker(
		c.Zk.Servers,
		engine,
		core.WithID(id),
		core.WithNamespace(c.Namespace),
		core.WithTimeout(timeout),
		core.WithLogger(logger))

	return w, nil
}

func initEngine(c *config.Config) (api.Engine, error) {
	return mysql.NewEngine(mysql.Config{
		Host:     c.DB.Host,
		Port:     c.DB.Port,
		User:     c.DB.User,
		Password: c.DB.Password,
		DBName:   c.DB.Database,
	})
}

func initLogger(id string, c *config.Config) commons.Logger {
	logLevel := zerolog.InfoLevel
	if c.Debug {
		logLevel = zerolog.DebugLevel
	}
	zerolog.SetGlobalLevel(logLevel)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	logger := zerolog.
		New(os.Stderr).
		With().
		Timestamp().
		Str("ns", c.Namespace).
		Str("wid", id).
		Logger().
		Output(zerolog.ConsoleWriter{Out: os.Stderr})
	return &internal.Logger{Logger: &logger}
}

func init() {
	rootCmd.AddCommand(workerCmd)
	workerCmd.Flags().StringP(envarg, "e", "local.env", "Config file for the config")
}
