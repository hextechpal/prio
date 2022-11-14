package cmd

import (
	"context"
	"fmt"
	"github.com/hextechpal/prio/internal/config"
	"github.com/hextechpal/prio/internal/handler"
	"github.com/hextechpal/prio/internal/router"
	"github.com/hextechpal/prio/internal/store/sql"
	"github.com/joho/godotenv"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"net/http"
	"os"
	"os/signal"
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

func setupServer(config *config.Config) {
	ctx := context.WithValue(context.Background(), "ns", config.Namespace)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logger := setupLogger(ctx, config)
	storage, err := sql.NewStorage(config.DB.Driver, config.DB.DSN, logger)
	if err != nil {
		panic(err)
	}

	r := router.New(config.Debug)
	r.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "Hello, World!")
	})

	g := r.Group("v1")
	h, err := handler.NewHandler(ctx, config, storage, logger)
	if err != nil {
		panic(err)
	}
	h.Register(g)

	// Start server
	go func() {
		err = r.Start(fmt.Sprintf("%s:%d", config.Server.Host, config.Server.Port))
		if err != nil && err != http.ErrServerClosed {
			logger.Fatal().Err(err).Msgf("shutting down the server")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	if err = r.Shutdown(ctx); err != nil {
		logger.Fatal().Err(err)
	}

}

func setupLogger(ctx context.Context, c *config.Config) *zerolog.Logger {
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
		Str("ns", ctx.Value("ns").(string)).
		Logger().
		Output(zerolog.ConsoleWriter{Out: os.Stderr})
	return &logger
}

func init() {
	rootCmd.AddCommand(workerCmd)
	workerCmd.Flags().StringP(envarg, "e", "local.env", "Config file for the config")
}
