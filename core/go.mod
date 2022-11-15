module github.com/hextechpal/prio/core

go 1.19

require (
	github.com/go-zookeeper/zk v1.0.3
	github.com/hextechpal/prio/commons v0.0.0-20221114132924-a785f445ba9f
)

require github.com/google/uuid v1.3.0 // indirect

//replace github.com/hextechpal/prio/commons => ../commons
