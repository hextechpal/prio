module github.com/hextechpal/prio/mysql-engine

go 1.19

require (
	github.com/golang-migrate/migrate/v4 v4.15.2
	github.com/hextechpal/prio/core v0.0.0-20221008075705-a7ff73ef86e8
	github.com/jmoiron/sqlx v1.3.5
)

require (
	github.com/go-sql-driver/mysql v1.6.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	go.uber.org/atomic v1.7.0 // indirect
)

replace github.com/hextechpal/prio/core => ../core
