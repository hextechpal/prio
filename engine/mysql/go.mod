module github.com/hextechpal/prio/engine/mysql

go 1.19

require (
	github.com/go-sql-driver/mysql v1.6.0
	github.com/hextechpal/prio/core v0.0.0-20221114132924-a785f445ba9f
	github.com/jmoiron/sqlx v1.3.5
	github.com/rs/zerolog v1.28.0
)

require (
	github.com/go-zookeeper/zk v1.0.3 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/hextechpal/prio/commons v0.0.0-20221114130552-743644f86e75 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	golang.org/x/sys v0.0.0-20221006211917-84dc82d7e875 // indirect
)

replace (
	github.com/hextechpal/prio/core => ../../core
)
