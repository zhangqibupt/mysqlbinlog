package mysqlbinlog

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"strings"
)

type rollbackEntry struct {
	MarkerID int64 // -1 means general SQL, >= 0 means rollback marker ID
	SQL      string
	DB       string // database name, used for auto increment reset
	Table    string // table name, used for auto increment reset
}

type RollbackSQL struct {
	sqls chan rollbackEntry
}

func (sql *RollbackSQL) appendGeneralSQLs(sqls []string, db, table string) {
	for _, s := range sqls {
		sql.sqls <- rollbackEntry{MarkerID: -1, SQL: s, DB: db, Table: table}
	}
}

func (sql *RollbackSQL) appendMarker(markerID int64) {
	sql.sqls <- rollbackEntry{MarkerID: markerID}
}

// Only concatenate rollback SQLs with ID <= markerID
func (sql *RollbackSQL) collectRollbackSQL(markerID int64) []string {
	var newSqls []string
	resetAutoIncrementTables := map[string]map[string]struct{}{} // map[db][table]bool, used to reset auto increment id
	// read from sql.sqls, utill markerID is reached
	for entry := range sql.sqls {
		// if not marker ID, then it is a general SQL
		if entry.MarkerID == -1 {
			if strings.Trim(entry.SQL, " \r\n") != "" {
				newSqls = append(newSqls, strings.Trim(entry.SQL, " \r\n"))
				if _, ok := resetAutoIncrementTables[entry.DB]; !ok {
					resetAutoIncrementTables[entry.DB] = make(map[string]struct{})
				}
				resetAutoIncrementTables[entry.DB][entry.Table] = struct{}{}
			} else {
				logrus.Warnf("Warning: empty SQL %s found in rollback entries, skipping it", entry.SQL)
			}
		} else {
			if entry.MarkerID != markerID {
				logrus.Panicf("Error: marker ID %d not match with expected %d, please check your binlog position", entry.MarkerID, markerID)
			}
			break
		}
	}

	// reset table auto increment id
	var setAutoIncrementSQLs []string
	for db, tbls := range resetAutoIncrementTables {
		for tb := range tbls {
			tbKey := getTableName(db, tb)
			tbInfo, ok := tableinfo.tableInfos[tbKey]
			if ok && tbInfo != nil {
				setAutoIncrementSQLs = append(setAutoIncrementSQLs, fmt.Sprintf(setAutoIncrementSQL, db, tb, tbInfo.AutoIncrement))
			}
		}
	}

	// return reversed SQLs
	return append(ReverseSlice(newSqls), setAutoIncrementSQLs...)
}

func (sql *RollbackSQL) begin() {
	markerID, err := insertMarkerID()
	if err != nil {
		logrus.Panicf("failed to insert marker id, err=%s", err.Error())
	}

	// 2. Collect rollback SQL
	sqls := rollbackSQL.collectRollbackSQL(markerID)

	// 3. Execute SQLs
	if len(sqls) > 0 {
		logrus.Warnf("starting a new rollback cycle with markerID=%d, the already collected SQLs below will be discarded(%s)", markerID, strings.Join(sqls, ";\n"))
	} else {
		logrus.Infof("starting a new rollback cycle with markerID=%d", markerID)
	}
}

var rollbackSQL = &RollbackSQL{
	sqls: make(chan rollbackEntry, 1000),
}
