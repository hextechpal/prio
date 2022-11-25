module github.com/hextechpal/prio/app

go 1.19

require (
	github.com/go-zookeeper/zk v1.0.3
	github.com/hextechpal/prio/core v0.0.0-20221125151452-105fca04192c
	github.com/hextechpal/prio/engine/memory v0.0.0-20221125151452-105fca04192c
	github.com/hextechpal/prio/engine/mysql v0.0.0-20221125151452-105fca04192c
	github.com/joho/godotenv v1.4.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/labstack/echo/v4 v4.9.1
	github.com/labstack/gommon v0.4.0
	github.com/rs/zerolog v1.28.0
	github.com/spf13/cobra v1.6.0
)

require (
	github.com/go-sql-driver/mysql v1.6.0 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.1 // indirect
	github.com/jmoiron/sqlx v1.3.5 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasttemplate v1.2.1 // indirect
	golang.org/x/crypto v0.0.0-20210817164053-32db794688a5 // indirect
	golang.org/x/net v0.0.0-20211015210444-4f30a5c0130f // indirect
	golang.org/x/sys v0.0.0-20221006211917-84dc82d7e875 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/time v0.0.0-20201208040808-7e3f01d25324 // indirect
)

//replace (
//	github.com/hextechpal/prio/core => ../core
//	github.com/hextechpal/prio/engine/memory => ../engine/memory
//	github.com/hextechpal/prio/engine/mysql => ../engine/mysql
//)
