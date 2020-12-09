package mysqlbinlog

import (
	"time"

	_ "github.com/pingcap/tidb/types/parser_driver"
)

type ConfCmd struct {
	Host               string
	Port               uint
	User               string
	Passwd             string
	StartFile          string
	BinlogTimeLocation *time.Location
	 // sometimes, mysql binlog event has delay
	 // to make sure all sqls are collected before next case, we will wait some time
	RollbackDelay      time.Duration
}

var confCmd *ConfCmd
