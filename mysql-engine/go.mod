module github.com/hextechpal/prio/mysql-engine

go 1.19

require (
	github.com/golang-migrate/migrate/v4 v4.15.2
	github.com/hextechpal/prio/commons v0.0.0-20221009073815-057673250264
	github.com/hextechpal/prio/core v0.0.0-20221008075705-a7ff73ef86e8
	github.com/jmoiron/sqlx v1.3.5
)

require (
	github.com/go-sql-driver/mysql v1.6.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/rs/zerolog v1.28.0 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	golang.org/x/sys v0.0.0-20221006211917-84dc82d7e875 // indirect
)

replace github.com/hextechpal/prio/core => ../core

replace github.com/hextechpal/prio/commons => ../commons
