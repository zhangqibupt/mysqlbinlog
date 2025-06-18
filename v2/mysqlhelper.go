package mysqlbinlog

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/sirupsen/logrus"
)

func getCurrentPosition() (pos mysql.Position, err error) {
	con := getDBCon()
	var res [5]string
	if err := con.QueryRow(showMasterStatusSQL).Scan(&res[0], &res[1], &res[2], &res[3], &res[4]); err != nil {
		return pos, err
	}
	p, err := strconv.Atoi(res[1])
	if err != nil {
		return pos, err
	}

	return mysql.Position{Name: res[0], Pos: uint32(p)}, nil
}

func mysqlUrl() string {
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/?autocommit=true&charset=utf8mb4,utf8,latin1&loc=Local&parseTime=true&multiStatements=true",
		confCmd.User, confCmd.Passwd, confCmd.Host, confCmd.Port)
}

func connectMysql(mysqlUrl string) (*sql.DB, error) {
	db, err := sql.Open("mysql", mysqlUrl)
	if err != nil {
		if db != nil {
			_ = db.Close()
		}
		return nil, err
	}

	if err = db.Ping(); err != nil {
		if db != nil {
			_ = db.Close()
		}
		return nil, err
	}

	return db, nil
}

var sqlCon *sql.DB

func getDBCon() *sql.DB {
	if sqlCon != nil {
		return sqlCon
	}
	con, err := connectMysql(mysqlUrl())
	if err != nil {
		logrus.Panicf("fail to connect to mysql, err=%s", err.Error())
	}

	if _, err := con.Exec(disableBinlogSQL); err != nil {
		logrus.Panicf("failed to disable binlog, err=%s", err.Error())
	}

	if _, err := con.Exec(disableKeyCheckSQL); err != nil {
		logrus.Panicf("failed to disable foreign key check, err=%s", err.Error())
	}

	sqlCon = con
	return sqlCon
}

func getTableNames() (map[string][]string, error) {
	logrus.Info("getting target table names from mysql")

	var (
		schema   string
		table    string
		dbTables = map[string][]string{}
	)

	rows, err := getDBCon().Query(getTableNamesSQL)
	if err != nil {
		return nil, err
	}
	if rows != nil {
		defer rows.Close()
	}

	for rows.Next() {
		if err = rows.Scan(&schema, &table); err != nil {
			return nil, err
		}
		if _, ok := dbTables[schema]; ok {
			dbTables[schema] = append(dbTables[schema], table)
		} else {
			dbTables[schema] = []string{table}
		}
	}
	return dbTables, nil
}

func getTableInfo() error {
	logrus.Info("start to get table structure from mysql")

	allTables, err := getTableNames()
	if err != nil {
		return fmt.Errorf("failed to get table names, err=%s", err.Error())
	}

	if err = tableinfo.getTableFields(allTables, 5000); err != nil {
		return fmt.Errorf("failed to get table fields, err=%s", err.Error())
	}

	if err = tableinfo.getTableKeys(allTables, 5000); err != nil {
		return fmt.Errorf("failed to get table keys, err=%s", err.Error())
	}

	if err = tableinfo.getTableAutoIncrements(allTables, 5000); err != nil {
		return fmt.Errorf("failed to get table auto_increments, err=%s", err.Error())
	}

	if len(tableinfo.tableInfos) == 0 {
		return fmt.Errorf("get no table difinition info from mysql, pls check user %s has privileges to read tables in infomation_schema", confCmd.User)
	}

	logrus.Info("successfully get table infos from db")
	return nil
}

func dropMarkerDB() error {
	// Drop marker database
	con := getDBCon()
	_, err := con.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", markerDatabaseName))
	if err != nil {
		return fmt.Errorf("failed to drop marker database: %s", err.Error())
	}
	logrus.Infof("Marker database %s dropped successfully", markerDatabaseName)
	return nil
}

var markerSqlCon *sql.DB

// binglog is enabled, used to manipulate marker database to add markers
func getMarkerDBCon() *sql.DB {
	if markerSqlCon != nil {
		return markerSqlCon
	}
	con, err := connectMysql(mysqlUrl())
	if err != nil {
		logrus.Panicf("fail to connect to mysql, err=%s", err.Error())
	}
	markerSqlCon = con
	return markerSqlCon
}

func initMarkerDB() error {
	// Create marker database and table if not exists
	con := getDBCon()

	// Drop existing database if exists
	_, err := con.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", markerDatabaseName))
	if err != nil {
		return fmt.Errorf("failed to drop existing marker database: %s", err.Error())
	}

	// Create new database
	_, err = con.Exec(fmt.Sprintf("CREATE DATABASE %s;", markerDatabaseName))
	if err != nil {
		return fmt.Errorf("failed to create marker database: %s", err.Error())
	}

	// Create marker table
	_, err = con.Exec(fmt.Sprintf(`CREATE TABLE %s.marker (
		id BIGINT AUTO_INCREMENT PRIMARY KEY
	);`, markerDatabaseName))
	if err != nil {
		return fmt.Errorf("failed to create marker table: %s", err.Error())
	}
	return nil
}
