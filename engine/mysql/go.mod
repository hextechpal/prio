module github.com/hextechpal/prio/engine/mysql

go 1.19

require (
	github.com/go-sql-driver/mysql v1.6.0
	github.com/hextechpal/prio/core v0.0.0-20221125150718-3fe15c6f3658
	github.com/jmoiron/sqlx v1.3.5
)

require github.com/google/uuid v1.3.0 // indirect

//replace github.com/hextechpal/prio/core => ../../core
