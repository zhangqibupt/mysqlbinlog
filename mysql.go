package mysqlbinlog

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/siddontang/go-mysql/mysql"
	"gopkg.in/volatiletech/null.v6"
)

func resetMaster() error {
	con := getDBCon()
	_, err := con.Exec(resetMasterSQL)
	return err
}

func disableBinlog() error {
	con := getDBCon()
	_, err := con.Exec(disableBinlogSQL)
	return err
}

func disableKeyCheck() error {
	con := getDBCon()
	_, err := con.Exec(disableKeyCheckSQL)
	return err
}

type fieldInfo struct {
	FieldName string `json:"column_name"`
	FieldType string `json:"column_type"`
}

type keyInfo []string //{colname1, colname2}

type tblInfoJson struct {
	Columns       []fieldInfo `json:"columns"`
	PrimaryKey    keyInfo     `json:"primary_key"`
	UniqueKeys    []keyInfo   `json:"unique_keys"`
	AutoIncrement uint64      `json:"auto_increment"`
}

type tablesColumnsInfo struct {
	tableInfos map[string]*tblInfoJson //{db.tb:TblInfoJson}
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
			db.Close()
		}
		return nil, err
	}

	if err = db.Ping(); err != nil {
		if db != nil {
			db.Close()
		}
		return nil, err
	}

	return db, nil
}

func (s tablesColumnsInfo) getTableInfo(schema string, table string, binlog string, spos uint32, epos uint32) (*tblInfoJson, error) {
	myPos := mysql.Position{Name: binlog, Pos: epos}
	tbKey := getTableName(schema, table)
	tbDef, ok := s.tableInfos[tbKey]
	if !ok {
		return nil, fmt.Errorf("table struct not found for %s, maybe it was dropped. Skip it, binlog position info: %s", tbKey, myPos.String())
	}
	return tbDef, nil
}

var sqlCon *sql.DB

func getDBCon() *sql.DB {
	if sqlCon != nil {
		return sqlCon
	}
	sqlUrl := mysqlUrl()
	con, err := connectMysql(sqlUrl)
	if err != nil {
		log.Fatalf("fail to connect to mysql, err=%s", err.Error())
	}
	sqlCon = con
	return sqlCon
}

func getColIndexFromKey(ki keyInfo, columns []fieldInfo) []int {
	arr := make([]int, len(ki))
	for j, colName := range ki {
		for i, f := range columns {
			if f.FieldName == colName {
				arr[j] = i
				break
			}
		}
	}
	return arr
}

func (s tblInfoJson) getOneUniqueKey() keyInfo {
	if len(s.PrimaryKey) > 0 {
		return s.PrimaryKey
	} else if len(s.UniqueKeys) > 0 {
		return s.UniqueKeys[0]
	} else {
		return keyInfo{}
	}
}

func (s *tablesColumnsInfo) checkAndCreateTblKey(schema, table string) bool {
	if len(s.tableInfos) < 1 {
		s.tableInfos = map[string]*tblInfoJson{}
	}
	tbKey := getTableName(schema, table)
	_, ok := s.tableInfos[tbKey]
	return ok
}

func (s *tablesColumnsInfo) getTableFields(dbTbs map[string][]string, batchCnt int) error {
	con := getDBCon()
	var (
		dbName         string
		tbName         string
		colName        string
		dataType       string
		colPos         int
		ok             bool
		querySqls      []string
		dbTbFieldsInfo = map[string]map[string][]fieldInfo{}
	)
	log.Println("geting table fields from mysql")
	querySqls = getFieldOrKeyQuerySqls(columnNamesTypesSQL, dbTbs, batchCnt)

	for _, oneQuery := range querySqls {
		rows, err := con.Query(oneQuery)
		if err != nil {
			log.Println("fail to query mysql: " + oneQuery)
			rows.Close()
			return err
		}

		for rows.Next() {
			if err := rows.Scan(&dbName, &tbName, &colName, &dataType, &colPos); err != nil {
				log.Println("error to get query result: " + oneQuery)
				rows.Close()
				return err
			}
			if _, ok = dbTbFieldsInfo[dbName]; !ok {
				dbTbFieldsInfo[dbName] = map[string][]fieldInfo{}
			}
			if _, ok = dbTbFieldsInfo[dbName][tbName]; !ok {
				dbTbFieldsInfo[dbName][tbName] = []fieldInfo{}
			}
			dbTbFieldsInfo[dbName][tbName] = append(dbTbFieldsInfo[dbName][tbName], fieldInfo{FieldName: colName, FieldType: dataType})

		}
		rows.Close()
	}

	for dbName = range dbTbFieldsInfo {
		for tbName, tbInfo := range dbTbFieldsInfo[dbName] {
			ok = s.checkAndCreateTblKey(dbName, tbName)
			tbKey := getTableName(dbName, tbName)
			if ok {
				s.tableInfos[tbKey].Columns = tbInfo
			} else {
				s.tableInfos[tbKey] = &tblInfoJson{Columns: tbInfo}
			}
		}
	}

	return nil
}

func (s *tablesColumnsInfo) getTableKeys(dbTbs map[string][]string, batchCnt int) error {
	con := getDBCon()
	var (
		dbName, tbName, kName, colName, ktype string
		colPos                                int
		ok                                    bool
		dbTbKeysInfo                          = map[string]map[string]map[string]keyInfo{}
		primaryKeys                           = map[string]map[string]map[string]bool{}
	)
	log.Println("geting primary/unique keys from mysql")
	//querySqls := GetFieldOrKeyQuerySqls(primaryUniqueKeysSqlBatch, dbTbs, batchCnt)
	querySqls := getFieldOrKeyQuerySqls(primaryUniqueKeysSQL, dbTbs, batchCnt)
	for _, oneQuery := range querySqls {
		rows, err := con.Query(oneQuery)
		if err != nil {
			rows.Close()
			log.Println("fail to query mysql: " + oneQuery)
			return err
		}

		for rows.Next() {
			//select k.table_schema, k.table_name, k.CONSTRAINT_NAME, k.COLUMN_NAME, c.CONSTRAINT_TYPE, k.ORDINAL_POSITION
			if err := rows.Scan(&dbName, &tbName, &kName, &colName, &ktype, &colPos); err != nil {
				log.Println("fail to get query result: " + oneQuery)
				rows.Close()
				return err
			}
			if _, ok = dbTbKeysInfo[dbName]; !ok {
				dbTbKeysInfo[dbName] = map[string]map[string]keyInfo{}
			}
			if _, ok = dbTbKeysInfo[dbName][tbName]; !ok {
				dbTbKeysInfo[dbName][tbName] = map[string]keyInfo{}
			}
			if _, ok = dbTbKeysInfo[dbName][tbName][kName]; !ok {
				dbTbKeysInfo[dbName][tbName][kName] = keyInfo{}
			}
			if !ContainsString(dbTbKeysInfo[dbName][tbName][kName], colName) {
				dbTbKeysInfo[dbName][tbName][kName] = append(dbTbKeysInfo[dbName][tbName][kName], colName)
			}

			if ktype == "PRIMARY KEY" {
				if _, ok = primaryKeys[dbName]; !ok {
					primaryKeys[dbName] = map[string]map[string]bool{}
				}
				if _, ok = primaryKeys[dbName][tbName]; !ok {
					primaryKeys[dbName][tbName] = map[string]bool{}
				}
				primaryKeys[dbName][tbName][kName] = true
			}

		}
		rows.Close()

	}

	var isPrimay = false
	for dbName = range dbTbKeysInfo {
		for tbName = range dbTbKeysInfo[dbName] {
			tbKey := getTableName(dbName, tbName)
			ok = s.checkAndCreateTblKey(dbName, tbName)
			if ok {
				s.tableInfos[tbKey].PrimaryKey = keyInfo{}
				s.tableInfos[tbKey].UniqueKeys = []keyInfo{}
			} else {
				s.tableInfos[tbKey] = &tblInfoJson{
					PrimaryKey: keyInfo{},
					UniqueKeys: []keyInfo{},
				}
			}
			for kn, kf := range dbTbKeysInfo[dbName][tbName] {
				isPrimay = false
				if _, ok = primaryKeys[dbName]; ok {
					if _, ok = primaryKeys[dbName][tbName]; ok {
						if v, ok := primaryKeys[dbName][tbName][kn]; ok && v {
							isPrimay = true
						}
					}
				}
				if isPrimay {
					s.tableInfos[tbKey].PrimaryKey = kf
				} else {
					s.tableInfos[tbKey].UniqueKeys = append(s.tableInfos[tbKey].UniqueKeys, kf)
				}
			}
		}
	}

	return nil
}

func (s *tablesColumnsInfo) getTableAutoIncrements(dbTbs map[string][]string, batchCnt int) error {
	con := getDBCon()
	log.Println("getting auto_increments from mysql")
	querySqls := getFieldOrKeyQuerySqls(autoIncrementsSQL, dbTbs, batchCnt)
	for _, oneQuery := range querySqls {
		rows, err := con.Query(oneQuery)
		if err != nil {
			rows.Close()
			log.Println("fail to query mysql: " + oneQuery)
			return err
		}
		for rows.Next() {
			var dbName, tbName string
			var autoIncr       null.Uint64
			if err := rows.Scan(&dbName, &tbName, &autoIncr); err != nil {
				log.Println("fail to get query result: " + oneQuery)
				rows.Close()
				return err
			}
			if !autoIncr.Valid {
				continue // skip this row, AUTO_INCREMENT may be NULL.
			}

			tbKey := getTableName(dbName, tbName)
			ok := s.checkAndCreateTblKey(dbName, tbName)
			if ok {
				s.tableInfos[tbKey].AutoIncrement = autoIncr.Uint64
			} else {
				s.tableInfos[tbKey] = &tblInfoJson{AutoIncrement: autoIncr.Uint64}
			}
		}
		rows.Close()
	}
	return nil
}

func getFieldOrKeyQuerySqls(sqlFmt string, dbTbs map[string][]string, batchCnt int) []string {
	var (
		querySqls []string
		oneSql    = ""
		db        string
		endIdx    int
		sidx      int
	)
	for db = range dbTbs {
		tbCnt := len(dbTbs[db])
		for sidx = 0; sidx < tbCnt; sidx += batchCnt {
			endIdx = sidx + batchCnt
			if endIdx >= tbCnt {
				endIdx = tbCnt
			}
			oneSql = fmt.Sprintf(sqlFmt, db, getStrCommaSep(dbTbs[db][sidx:endIdx]))
			querySqls = append(querySqls, oneSql)
		}

	}

	return querySqls
}

func getStrCommaSep(arr []string) string {
	arrTmp := make([]string, len(arr))
	for i, v := range arr {
		arrTmp[i] = fmt.Sprintf("'%s'", v)
	}
	return strings.Join(arrTmp, ",")
}

func getTableNames() (map[string][]string, error) {
	log.Println("geting target table names from mysql")

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
	log.Println("start to get table structure from mysql")

	allTables, err := getTableNames()
	if err != nil {
		return fmt.Errorf("failed to get table names, err=%s", err.Error())
	}

	if err = tableinfo.getTableFields(allTables, 50); err != nil {
		return fmt.Errorf("failed to get table fields, err=%s", err.Error())
	}

	if err = tableinfo.getTableKeys(allTables, 50); err != nil {
		return fmt.Errorf("failed to get table keys, err=%s", err.Error())
	}

	if err = tableinfo.getTableAutoIncrements(allTables, 50); err != nil {
		return fmt.Errorf("failed to get table auto_increments, err=%s", err.Error())
	}

	if len(tableinfo.tableInfos) == 0 {
		return fmt.Errorf("get no table difinition info from mysql, pls check user %s has privileges to read tables in infomation_schema", confCmd.User)
	}

	log.Println("successfully get table infos from db")
	return nil
}
