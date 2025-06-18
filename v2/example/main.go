package main

import (
	"github.com/sirupsen/logrus"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.freewheel.tv/bricks/mysqlbinlog/v2"
)

func main() {
	var (
		host = "localhost"
		port = uint(3306)
		user = "root"
		pwd  = "root"
	)

	// start to listen mysql binlog
	err := mysqlbinlog.Start(host, port, user, pwd)
	if err != nil {
		logrus.Panicf("Error to listen mysql binlog, err=%v", err)
	}

	// run case 0 - 9
	for i := 0; i < 10; i++ {
		mysqlbinlog.Begin()
		// run case, execute INSERT, DELETE, UPDATE
		time.Sleep(time.Second * 2)

		// rollback
		mysqlbinlog.Rollback()
	}

	// stop listening before exit
	mysqlbinlog.Stop()
}
