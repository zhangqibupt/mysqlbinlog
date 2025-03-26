package mysqlbinlog

import (
	"fmt"
	"strings"
	"sync"
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
	RollbackDelay time.Duration
}

var confCmd *ConfCmd

var skipTables sync.Map

func AddSkipTables(tables ...string) (err error) {
	for _, t := range tables {
		if parts := strings.Split(t, "."); len(parts) != 2 {
			return fmt.Errorf("invalid table name: %v, must be schema.table", t)
		}
		skipTables.Store(t, true)
	}
	return
}

func RemoveSkipTables(tables ...string) {
	for _, t := range tables {
		skipTables.Delete(t)
	}
}

func shouldSkipTable(table string) bool {
	_, found := skipTables.Load(table)
	return found
}
