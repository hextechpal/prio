module github.com/hextechpal/prio/worker

go 1.19

require (
	github.com/go-zookeeper/zk v1.0.3
	github.com/hextechpal/prio/commons v0.0.0-20221012043015-6a93205fc377
	github.com/hextechpal/prio/core v0.0.0-20221012043015-6a93205fc377
)

require (
	github.com/google/uuid v1.3.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/rs/zerolog v1.28.0 // indirect
	golang.org/x/sys v0.0.0-20221006211917-84dc82d7e875 // indirect
)

replace github.com/hextechpal/prio/commons => ../commons

replace github.com/hextechpal/prio/core => ../core
