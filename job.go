package mysqlbinlog

import (
	"fmt"
	"log"
	"strings"
	"time"
)

func Start(host string, port uint, user string, password string, duration time.Duration) error {
	// this is to align datetime with DB config, or the rollback sql will have +8:00 offset
	lo, err := time.LoadLocation("")
	if err != nil {
		return fmt.Errorf("failed to load UTC timezone, err=%s", err.Error())
	}

	confCmd = &ConfCmd{
		Host:               host,
		Port:               port,
		User:               user,
		Passwd:             password,
		BinlogTimeLocation: lo,
		RollbackDelay:      duration,
	}

	if err := resetMaster(); err != nil {
		return fmt.Errorf("failed to reset master binlog, err=%s", err.Error())
	}

	if err := disableBinlog(); err != nil {
		return fmt.Errorf("failed to disable binlog for current connection, err=%s", err.Error())
	}

	if err := disableKeyCheck(); err != nil {
		return fmt.Errorf("failed to disable key check, err=%s", err.Error())
	}

	if err := getTableInfo(); err != nil {
		return fmt.Errorf("failed to get table info, err=%s", err.Error())
	}

	go startListenBinEvents()
	go startGenRollbackSql()
	return nil
}

func Stop() {
	if sqlCon != nil {
		sqlCon.Close()
	}
}

func Rollback() {
	con := getDBCon()
	sql := rollbackSQL.concatRollbackSql()
	if len(strings.Trim(sql, " \r\n")) != 0 {
		if _, err := con.Exec(sql); err != nil {
			log.Fatalf("failed to rollback sql, sql= %s err=%s", sql, err.Error())
		}
	}
}