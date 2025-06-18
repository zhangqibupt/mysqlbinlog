package mysqlbinlog

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/manilion/godropbox/database/sqlbuilder"
	SQL "github.com/manilion/godropbox/database/sqlbuilder"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/sirupsen/logrus"
	"strings"
)

func startGenRollbackSql() {
	var (
		err            error
		tbInfo         *tblInfoJson
		db, tb, fulltb string
		allColNames    []fieldInfo
		colsDef        []SQL.NonAliasColumn
		colsTypeName   []string
		colCnt         int
		sqls           []string
		uniqueKeyIdx   []int
		uniqueKey      keyInfo
		posStr         string
	)
	logrus.Info("start to generate rollback sql")

	for ev := range eventChan {
		if !ev.IfRowsEvent {
			continue
		}

		sqls = []string{}
		posStr = getPosStr(ev.MyPos.Name, ev.StartPos, ev.MyPos.Pos)
		db = string(ev.BinEvent.Table.Schema)
		tb = string(ev.BinEvent.Table.Table)
		fulltb = getTableName(db, tb)

		var colsTypeNameFromMysql []string
		canRetry := true
		// Fix issue: can not find table or table fields if table structure changes during cases are running
		for {
			tbInfo, err = tableinfo.getTableInfo(db, tb, ev.MyPos.Name, ev.StartPos, ev.MyPos.Pos)
			if err != nil {
				logrus.Panicf("error to found %s table structure for event %s", fulltb, posStr)
			}
			// when new table added, we need to update the table defination via `getTableInfo` and retry
			if tbInfo == nil {
				msg := fmt.Sprintf("no suitable table struct found for %s for event %s", fulltb, posStr)

				if !canRetry {
					logrus.Panicf(msg)
				}

				canRetry = false
				if err = getTableInfo(); err != nil {
					logrus.Panicf(err.Error())
				}
				continue
			}

			colCnt = len(ev.BinEvent.Rows[0])
			allColNames = getAllFieldNamesWithDroppedFields(colCnt, tbInfo.Columns)
			allColNames, ev.BinEvent.Rows = filterStoredGeneratedFields(allColNames, ev.BinEvent.Rows)

			colCnt = len(ev.BinEvent.Rows[0])
			colsDef, colsTypeName = getSqlFieldsExpressions(colCnt, allColNames, ev.BinEvent.Table)
			colsTypeNameFromMysql = make([]string, len(colsTypeName))
			if len(colsTypeName) <= len(tbInfo.Columns) {
				break
			}

			// when table fields changed, we need to update table structure definition via `getTableInfo` and retry
			msg := fmt.Sprintf("column count %d in binlog > in table structure %d, usually means DDL in the middle, pls generate a suitable table structure table=%s\nbinlog=%s\ntable structure:\n\t%s\nrow values:\n\t%s",
				len(colsTypeName), len(tbInfo.Columns), fulltb, ev.MyPos.String(), spew.Sdump(tbInfo.Columns), spew.Sdump(ev.BinEvent.Rows[0]))

			if !canRetry {
				logrus.Panicf(msg)
			}

			canRetry = false
			logrus.Info(msg)
			if err = getTableInfo(); err != nil {
				logrus.Panicf(err.Error())
			}
		}

		// convert blob type to string
		for ci, colType := range colsTypeName {
			colsTypeNameFromMysql[ci] = tbInfo.Columns[ci].FieldType
			if colType == BLOB {
				// text is stored as blob
				if strings.Contains(strings.ToLower(tbInfo.Columns[ci].FieldType), "text") {
					for ri := range ev.BinEvent.Rows {
						if ev.BinEvent.Rows[ri][ci] == nil {
							continue
						}
						txtStr, coOk := ev.BinEvent.Rows[ri][ci].([]byte)
						if !coOk {
							logrus.Panicf("fail to convert %s to []byte type", ev.BinEvent.Rows[ri][ci])
						} else {
							ev.BinEvent.Rows[ri][ci] = string(txtStr)
						}
					}
				}
			}
		}
		uniqueKey = tbInfo.getOneUniqueKey()
		if len(uniqueKey) > 0 {
			uniqueKeyIdx = getColIndexFromKey(uniqueKey, allColNames)
		} else {
			uniqueKeyIdx = []int{}
		}

		if fulltb == markerDatabaseTableFullName {
			if ev.SqlType == SQLTypeInsert {
				if len(ev.BinEvent.Rows) != 1 {
					logrus.Panicf("Error: marker table %s should only have one row inserted, but got %d at position %s", markerDatabaseTableFullName, len(ev.BinEvent.Rows), posStr)
				}

				markerID := ev.BinEvent.Rows[0][0].(int64)
				rollbackSQL.appendMarker(markerID)
			} else {
				logrus.Infof("Error: marker table %s should only be inserted, but got %v at position %s", markerDatabaseTableFullName, ev.SqlType, posStr)
			}
			continue
		}

		if ev.SqlType == SQLTypeInsert {
			sqls = genDeleteSqls(posStr, ev.BinEvent, colsDef, uniqueKeyIdx, false, true)
		} else if ev.SqlType == SQLTypeDelete {
			sqls = genInsertSqls(posStr, ev.BinEvent, colsDef, 20, true)
		} else if ev.SqlType == SQLTypeUpdate {
			sqls = genUpdateSqls(posStr, colsTypeNameFromMysql, colsTypeName, ev.BinEvent, colsDef, uniqueKeyIdx, false, true)
		} else {
			logrus.Infof("Warning: unsupported query type %d to generate rollback sql, it should one of insert|update|delete. %s", ev.SqlType, ev.MyPos.String())
			continue
		}
		rollbackSQL.appendGeneralSQLs(sqls, db, tb)
	}
}

func filterStoredGeneratedFields(names []fieldInfo, rows [][]interface{}) ([]fieldInfo, [][]interface{}) {
	var (
		newNames []fieldInfo
		newRows  [][]interface{}
	)

	// find removal index ids
	removalIds := make([]int, 0, len(names))
	for i, col := range names {
		if col.Extra == "STORED GENERATED" {
			removalIds = append(removalIds, i)
		}
	}
	// remove columns
	for i, col := range names {
		if !ContainsInt(removalIds, i) {
			newNames = append(newNames, col)
		}
	}

	for _, row := range rows {
		newRow := make([]interface{}, 0, len(row))
		for i, col := range row {
			if !ContainsInt(removalIds, i) {
				newRow = append(newRow, col)
			}
		}
		newRows = append(newRows, newRow)
	}

	return newNames, newRows
}

func getMysqlDataTypeNameAndSqlColumn(tpDef string, colName string, tp byte, meta uint16) (string, sqlbuilder.NonAliasColumn) {
	// get real string type
	if tp == mysql.MYSQL_TYPE_STRING {
		if meta >= 256 {
			b0 := uint8(meta >> 8)
			if b0&0x30 != 0x30 {
				tp = b0 | 0x30
			} else {
				tp = b0
			}
		}
	}
	switch tp {
	case mysql.MYSQL_TYPE_NULL:
		return CUnknowncoltype, sqlbuilder.BytesColumn(colName, sqlbuilder.NotNullable)
	case mysql.MYSQL_TYPE_LONG:
		return "int", sqlbuilder.IntColumn(colName, sqlbuilder.NotNullable)

	case mysql.MYSQL_TYPE_TINY:
		return "tinyint", sqlbuilder.IntColumn(colName, sqlbuilder.NotNullable)

	case mysql.MYSQL_TYPE_SHORT:
		return "smallint", sqlbuilder.IntColumn(colName, sqlbuilder.NotNullable)

	case mysql.MYSQL_TYPE_INT24:
		return "mediumint", sqlbuilder.IntColumn(colName, sqlbuilder.NotNullable)

	case mysql.MYSQL_TYPE_LONGLONG:
		return "bigint", sqlbuilder.IntColumn(colName, sqlbuilder.NotNullable)

	case mysql.MYSQL_TYPE_NEWDECIMAL:
		return "decimal", sqlbuilder.DoubleColumn(colName, sqlbuilder.NotNullable)

	case mysql.MYSQL_TYPE_FLOAT:
		return "float", sqlbuilder.DoubleColumn(colName, sqlbuilder.NotNullable)
	case mysql.MYSQL_TYPE_DOUBLE:
		return "double", sqlbuilder.DoubleColumn(colName, sqlbuilder.NotNullable)
	case mysql.MYSQL_TYPE_BIT:
		return "bit", sqlbuilder.IntColumn(colName, sqlbuilder.NotNullable)
	case mysql.MYSQL_TYPE_TIMESTAMP:
		return "timestamp", sqlbuilder.StrColumn(colName, sqlbuilder.UTF8, sqlbuilder.UTF8CaseInsensitive, sqlbuilder.NotNullable)
	case mysql.MYSQL_TYPE_TIMESTAMP2:
		return "timestamp", sqlbuilder.StrColumn(colName, sqlbuilder.UTF8, sqlbuilder.UTF8CaseInsensitive, sqlbuilder.NotNullable)
	case mysql.MYSQL_TYPE_DATETIME:
		return "timestamp", sqlbuilder.StrColumn(colName, sqlbuilder.UTF8, sqlbuilder.UTF8CaseInsensitive, sqlbuilder.NotNullable)
	case mysql.MYSQL_TYPE_DATETIME2:
		return "timestamp", sqlbuilder.StrColumn(colName, sqlbuilder.UTF8, sqlbuilder.UTF8CaseInsensitive, sqlbuilder.NotNullable)
	case mysql.MYSQL_TYPE_TIME:
		return "time", sqlbuilder.StrColumn(colName, sqlbuilder.UTF8, sqlbuilder.UTF8CaseInsensitive, sqlbuilder.NotNullable)
	case mysql.MYSQL_TYPE_TIME2:
		return "time", sqlbuilder.StrColumn(colName, sqlbuilder.UTF8, sqlbuilder.UTF8CaseInsensitive, sqlbuilder.NotNullable)
	case mysql.MYSQL_TYPE_DATE:
		return "date", sqlbuilder.StrColumn(colName, sqlbuilder.UTF8, sqlbuilder.UTF8CaseInsensitive, sqlbuilder.NotNullable)

	case mysql.MYSQL_TYPE_YEAR:
		return "year", sqlbuilder.IntColumn(colName, sqlbuilder.NotNullable)
	case mysql.MYSQL_TYPE_ENUM:
		return "enum", sqlbuilder.IntColumn(colName, sqlbuilder.NotNullable)
	case mysql.MYSQL_TYPE_SET:
		return "set", sqlbuilder.IntColumn(colName, sqlbuilder.NotNullable)
	case mysql.MYSQL_TYPE_BLOB:
		if strings.Contains(strings.ToLower(tpDef), "text") {
			return BLOB, sqlbuilder.StrColumn(colName, sqlbuilder.UTF8, sqlbuilder.UTF8CaseInsensitive, sqlbuilder.NotNullable)
		}
		return BLOB, sqlbuilder.BytesColumn(colName, sqlbuilder.NotNullable)
	case mysql.MYSQL_TYPE_VARCHAR,
		mysql.MYSQL_TYPE_VAR_STRING:

		return "varchar", sqlbuilder.StrColumn(colName, sqlbuilder.UTF8, sqlbuilder.UTF8CaseInsensitive, sqlbuilder.NotNullable)
	case mysql.MYSQL_TYPE_STRING:
		return "char", sqlbuilder.StrColumn(colName, sqlbuilder.UTF8, sqlbuilder.UTF8CaseInsensitive, sqlbuilder.NotNullable)
	case mysql.MYSQL_TYPE_JSON:
		return "json", sqlbuilder.BytesColumn(colName, sqlbuilder.NotNullable)
	case mysql.MYSQL_TYPE_GEOMETRY:
		return "geometry", sqlbuilder.BytesColumn(colName, sqlbuilder.NotNullable)
	default:
		return CUnknowncoltype, sqlbuilder.BytesColumn(colName, sqlbuilder.NotNullable)
	}
}

func getDroppedFieldName(idx int) string {
	return fmt.Sprintf("%s%d", CUnknowncolprefix, idx)
}

func getAllFieldNamesWithDroppedFields(rowLen int, colNames []fieldInfo) []fieldInfo {
	if rowLen <= len(colNames) {
		return colNames
	}
	var arr = make([]fieldInfo, rowLen)
	cnt := copy(arr, colNames)
	for i := cnt; i < rowLen; i++ {
		arr[i] = fieldInfo{FieldName: getDroppedFieldName(i - cnt), FieldType: CUnknowncoltype}
	}
	return arr
}

func getSqlFieldsExpressions(colCnt int, colNames []fieldInfo, tbMap *replication.TableMapEvent) ([]sqlbuilder.NonAliasColumn, []string) {
	colDefExps := make([]sqlbuilder.NonAliasColumn, colCnt)
	colTypeNames := make([]string, colCnt)
	for i := 0; i < colCnt; i++ {
		typeName, colDef := getMysqlDataTypeNameAndSqlColumn(colNames[i].FieldType, colNames[i].FieldName, tbMap.ColumnType[i], tbMap.ColumnMeta[i])
		colDefExps[i] = colDef
		colTypeNames[i] = typeName
	}
	return colDefExps, colTypeNames
}

func genEqualConditions(row []interface{}, colDefs []sqlbuilder.NonAliasColumn, uniKey []int, ifFullImage bool) []sqlbuilder.BoolExpression {
	if !ifFullImage && len(uniKey) > 0 {
		expArrs := make([]sqlbuilder.BoolExpression, len(uniKey))
		for k, idx := range uniKey {
			expArrs[k] = sqlbuilder.EqL(colDefs[idx], row[idx])
		}
		return expArrs
	}
	expArrs := make([]sqlbuilder.BoolExpression, len(row))
	for i, v := range row {
		expArrs[i] = sqlbuilder.EqL(colDefs[i], v)
	}
	return expArrs
}

func convertRowToExpressRow(row []interface{}, ifIgnorePrimary bool, primaryIdx []int) []sqlbuilder.Expression {
	var valueInserted []sqlbuilder.Expression
	for i, val := range row {
		if ifIgnorePrimary {
			if ContainsInt(primaryIdx, i) {
				continue
			}
		}
		vExp := sqlbuilder.Literal(val)
		valueInserted = append(valueInserted, vExp)
	}
	return valueInserted
}

func genInsertSqlForRows(rows [][]interface{}, insertSql sqlbuilder.InsertStatement, schema string, ifprefixDb bool, ifIgnorePrimary bool, primaryIdx []int) (string, error) {
	for _, row := range rows {
		valuesInserted := convertRowToExpressRow(row, ifIgnorePrimary, primaryIdx)
		insertSql.Add(valuesInserted...)
	}
	if !ifprefixDb {
		schema = ""
	}
	return insertSql.String(schema)
}

func genInsertSqls(posStr string, rEv *replication.RowsEvent, colDefs []sqlbuilder.NonAliasColumn, rowsPerSql int, ifprefixDb bool) []string {
	var (
		insertSql  sqlbuilder.InsertStatement
		oneSql     string
		err        error
		i          int
		endIndex   int
		newColDefs = colDefs[:]
		rowCnt     = len(rEv.Rows)
		schema     = string(rEv.Table.Schema)
		table      = string(rEv.Table.Table)
		sqlArr     []string
		sqlType    string
	)

	sqlType = "insert_for_delete_rollback"
	for i = 0; i < rowCnt; i += rowsPerSql {
		insertSql = sqlbuilder.NewTable(table, newColDefs...).Insert(newColDefs...)
		endIndex = MinValue(rowCnt, i+rowsPerSql)
		oneSql, err = genInsertSqlForRows(rEv.Rows[i:endIndex], insertSql, schema, ifprefixDb, false, []int{})
		if err != nil {
			logrus.Infof("Error: Fail to generate for %v due to %s", rEv.Rows[i:endIndex], err.Error())
		} else {
			sqlArr = append(sqlArr, oneSql)
		}
	}

	if endIndex < rowCnt {
		insertSql = sqlbuilder.NewTable(table, newColDefs...).Insert(newColDefs...)
		oneSql, err = genInsertSqlForRows(rEv.Rows[endIndex:rowCnt], insertSql, schema, ifprefixDb, false, []int{})
		if err != nil {
			logrus.Infof("Error: Fail to generate %s sql for %s %s \n\terror: %s\n\trows data:%v", sqlType, getTableName(schema, table), posStr, err, rEv.Rows[endIndex:rowCnt])
		} else {
			sqlArr = append(sqlArr, oneSql)
		}
	}
	return sqlArr
}

func genDeleteSqls(posStr string, rEv *replication.RowsEvent, colDefs []sqlbuilder.NonAliasColumn, uniKey []int, ifFullImage bool, ifprefixDb bool) []string {
	rowCnt := len(rEv.Rows)
	sqlArr := make([]string, rowCnt)
	schema := string(rEv.Table.Schema)
	table := string(rEv.Table.Table)
	schemaInSql := schema
	if !ifprefixDb {
		schemaInSql = ""
	}

	for i, row := range rEv.Rows {
		whereCond := genEqualConditions(row, colDefs, uniKey, ifFullImage)

		sql, err := sqlbuilder.NewTable(table, colDefs...).Delete().Where(sqlbuilder.And(whereCond...)).String(schemaInSql)
		if err != nil {
			logrus.Infof("Error: Fail to generate %s sql for delete_for_insert_rollback %s \n\terror: %s\n\trows data:%v", getTableName(schema, table), posStr, err, row)
		}
		sqlArr[i] = sql
	}
	return sqlArr
}

func genUpdateSetPart(colsTypeNameFromMysql []string, colTypeNames []string, updateSql sqlbuilder.UpdateStatement, colDefs []sqlbuilder.NonAliasColumn, rowAfter []interface{}, rowBefore []interface{}, ifFullImage bool) sqlbuilder.UpdateStatement {

	ifUpdateCol := false
	for i, v := range rowAfter {
		ifUpdateCol = false

		if !ifFullImage {
			// text is stored as blob in binlog
			if ContainsString(bytesColumnTypes, colTypeNames[i]) && !strings.Contains(strings.ToLower(colsTypeNameFromMysql[i]), "text") {
				aArr, aOk := v.([]byte)
				bArr, bOk := rowBefore[i].([]byte)
				if aOk && bOk {
					if CompareEquelByteSlice(aArr, bArr) {
						ifUpdateCol = false
					} else {
						ifUpdateCol = true
					}
				} else {
					//should update the column
					ifUpdateCol = true
				}

			} else {
				if v == rowBefore[i] {
					ifUpdateCol = false
				} else {
					ifUpdateCol = true
				}
			}
		} else {
			ifUpdateCol = true
		}

		if ifUpdateCol {
			updateSql.Set(colDefs[i], sqlbuilder.Literal(v))
		}
	}
	return updateSql

}

func genUpdateSqls(posStr string, colsTypeNameFromMysql []string, colsTypeName []string, rEv *replication.RowsEvent, colDefs []sqlbuilder.NonAliasColumn, uniKey []int, ifFullImage bool, ifprefixDb bool) []string {
	//colsTypeNameFromMysql: for text type, which is stored as blob
	var (
		rowCnt      = len(rEv.Rows)
		schema      = string(rEv.Table.Schema)
		table       = string(rEv.Table.Table)
		schemaInSql = schema
		sqlArr      []string
		sql         string
		err         error
		wherePart   []sqlbuilder.BoolExpression
	)

	if !ifprefixDb {
		schemaInSql = ""
	}

	for i := 0; i < rowCnt; i += 2 {
		upSql := sqlbuilder.NewTable(table, colDefs...).Update()
		upSql = genUpdateSetPart(colsTypeNameFromMysql, colsTypeName, upSql, colDefs, rEv.Rows[i], rEv.Rows[i+1], ifFullImage)
		wherePart = genEqualConditions(rEv.Rows[i+1], colDefs, uniKey, ifFullImage)
		upSql.Where(sqlbuilder.And(wherePart...))
		sql, err = upSql.String(schemaInSql)
		if err != nil {
			logrus.Infof("Error: Fail to generate update_for_update_rollback sql for %s %s \n\terror: %s\n\trows data:%v\n%v", getTableName(schema, table), posStr, err, rEv.Rows[i], rEv.Rows[i+1])
			continue
		}
		sqlArr = append(sqlArr, sql)
	}
	return sqlArr
}

func getPosStr(name string, spos uint32, epos uint32) string {
	return fmt.Sprintf("%s %d-%d", name, spos, epos)
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
