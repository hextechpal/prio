package mysql

import "fmt"

type Config struct {
	Host     string
	Port     int32
	User     string
	Password string
	DBName   string
}

func (c Config) dsn() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.User, c.Password, c.Host, c.Port, c.DBName)
}
