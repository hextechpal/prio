module github.com/hextechpal/prio/example

go 1.19

require (
	github.com/go-zookeeper/zk v1.0.3
	github.com/hextechpal/prio/commons v0.0.0-20221012043015-6a93205fc377
	github.com/hextechpal/prio/sql-engine v0.0.0-20221015095014-cd05c01abe8f
	github.com/hextechpal/prio/worker v0.0.0-20221015095014-cd05c01abe8f
)

require (
	github.com/go-sql-driver/mysql v1.6.0 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/hextechpal/prio/core v0.0.0-20221015094846-9a86a605d7b5 // indirect
	github.com/jmoiron/sqlx v1.3.5 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/rs/zerolog v1.28.0 // indirect
	golang.org/x/sys v0.0.0-20221006211917-84dc82d7e875 // indirect
)

replace (
	github.com/hextechpal/prio/commons => ../commons
	github.com/hextechpal/prio/core => ../core
	github.com/hextechpal/prio/sql-engine => ../sql-engine
	github.com/hextechpal/prio/worker => ../worker
)
