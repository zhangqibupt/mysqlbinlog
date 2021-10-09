package main

import (
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.freewheel.tv/bricks/mysqlbinlog"
)

func main() {
	var (
		host  = "localhost"
		port  = uint(3306)
		user  = "root"
		pwd   = "root"
		delay = time.Millisecond * 300 // wait some time before rollback because the binlog may have some delay
	)

	// start to listen mysql binlog
	err := mysqlbinlog.Start(host, port, user, pwd, delay)
	if err != nil {
		log.Fatalf("Error to listen mysql binlog, err=%v", err)
	}

	// run case 0 - 9
	for i := 0; i < 10; i++ {
		// run case, execute INSERT, DELETE, UPDATE
		time.Sleep(time.Second * 2)

		// rollback
		mysqlbinlog.Rollback()
	}

	// stop listening before exit
	mysqlbinlog.Stop()
}
