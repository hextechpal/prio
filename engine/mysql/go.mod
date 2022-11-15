module github.com/hextechpal/prio/engine/mysql

go 1.19

require (
	github.com/go-sql-driver/mysql v1.6.0
	github.com/jmoiron/sqlx v1.3.5
	github.com/rs/zerolog v1.28.0
)

require (
	github.com/go-zookeeper/zk v1.0.3 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	golang.org/x/sys v0.0.0-20221006211917-84dc82d7e875 // indirect
)

//replace github.com/hextechpal/prio/core => ../../core
