package commons

import "log"

type Logger interface {
	Printf(format string, v ...any)
	Info(format string, v ...any)
	Error(err error, format string, v ...any)
	Warn(format string, v ...any)
	Debug(format string, v ...any)
	Fatal(format string, v ...any)
}

type DefaultLogger struct {
}

func (d *DefaultLogger) Printf(format string, v ...any) {

	log.Printf(format, v...)
}

func (d *DefaultLogger) Info(format string, v ...any) {
	log.Printf(format, v...)
}

func (d *DefaultLogger) Error(err error, format string, v ...any) {
	log.Printf("error=%s", err.Error())
	log.Printf(format, v...)

}

func (d *DefaultLogger) Warn(format string, v ...any) {
	log.Printf(format, v...)
}

func (d *DefaultLogger) Debug(format string, v ...any) {
	log.Printf(format, v...)
}

func (d *DefaultLogger) Fatal(format string, v ...any) {
	log.Fatalf(format, v...)
}
