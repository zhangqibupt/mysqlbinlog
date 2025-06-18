package mysqlbinlog

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

const markerDatabaseName = "_mysqlbinlog_marker_db"
const markerDatabaseTableFullName = "_mysqlbinlog_marker_db.marker"

func Start(host string, port uint, user string, password string) error {
	// Configure logrus
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	}) // You can change to &logrus.JSONFormatter{} for JSON logs
	logrus.SetOutput(os.Stdout)       // You can change to a file for file logging
	logrus.SetLevel(logrus.InfoLevel) // Set the desired log level (e.g., DebugLevel, InfoLevel, WarnLevel, ErrorLevel)

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
	}

	// this should happen before getTableInfo
	if err := initMarkerDB(); err != nil {
		return fmt.Errorf("failed to init marker db, err=%s", err.Error())
	}

	if err := getTableInfo(); err != nil {
		return fmt.Errorf("failed to get table info, err=%s", err.Error())
	}

	pos, err := getCurrentPosition()
	if err != nil {
		return fmt.Errorf("failed to get binlog current position, err=%s", err.Error())
	}

	go startListenBinEvents(pos)
	go startGenRollbackSql()
	return nil
}

func Stop() {
	if sqlCon != nil {
		// Clean up marker table
		if err := dropMarkerDB(); err != nil {
			logrus.Errorf("failed to drop marker db, err=%s", err.Error())
		}
		// Close the connection
		logrus.Infof("closing connection to MySQL server %s:%d", confCmd.Host, confCmd.Port)
		if err := sqlCon.Close(); err != nil {
			logrus.Errorf("failed to close connection, err=%s", err.Error())
		} else {
			logrus.Infof("connection to MySQL server %s:%d closed", confCmd.Host, confCmd.Port)
		}
		sqlCon = nil
	} else {
		logrus.Infof("no connection to close, MySQL server %s:%d", confCmd.Host, confCmd.Port)
	}
}

func Rollback() {
	markerID, err := insertMarkerID()
	if err != nil {
		logrus.Panicf("failed to insert marker id, err=%s", err.Error())
	}

	// 2. Collect rollback SQL
	sqls := rollbackSQL.collectRollbackSQL(markerID)

	// 3. Execute SQLs
	if len(sqls) > 0 {
		sqlString := strings.Join(sqls, ";")
		if _, err := getDBCon().Exec(sqlString); err != nil {
			logrus.Panicf("failed to rollback sql, sql= %s err=%s", sqlString, err.Error())
		}
		logrus.Infof("rollback executed successfully, markerID=%d, sql count=%d", markerID, len(sqls))
		logrus.Debugf("rollback SQLs executed: %s", sqlString)
	} else {
		logrus.Infof("no rollback SQLs to execute, markerID=%d", markerID)
	}
}

func insertMarkerID() (int64, error) {
	con := getMarkerDBCon()
	// 1. Insert marker and get its id
	res, err := con.Exec(fmt.Sprintf("INSERT INTO %s () VALUES ();", markerDatabaseTableFullName))
	if err != nil {
		return 0, fmt.Errorf("failed to insert marker: %s", err.Error())
	}
	markerID, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get marker id: %s", err.Error())
	}
	return markerID, nil
}

func Begin() {
	rollbackSQL.begin()
}
