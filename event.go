package mysqlbinlog

import (
	"context"
	"log"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

type myBinEvent struct {
	MyPos       mysql.Position //this is the end position
	BinEvent    *replication.RowsEvent
	StartPos    uint32 // this is the start position
	IfRowsEvent bool
	SqlType     SQLType // insert, update, delete
}

func (s *myBinEvent) checkBinEvent(cfg *ConfCmd, ev *replication.BinlogEvent, currentBinlog *string) int {
	myPos := mysql.Position{Name: *currentBinlog, Pos: ev.Header.LogPos}
	switch ev.Header.EventType {
	case replication.ROTATE_EVENT:
		log.Printf("log rotate %s", myPos.String())
		rotatEvent := ev.Event.(*replication.RotateEvent)
		*currentBinlog = string(rotatEvent.NextLogName)
		myPos.Name = string(rotatEvent.NextLogName)
		myPos.Pos = uint32(rotatEvent.Position)
		s.IfRowsEvent = false
		return CRecontinue
	case replication.WRITE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv2:
		wrEvent := ev.Event.(*replication.RowsEvent)
		s.BinEvent = wrEvent
		s.IfRowsEvent = true
		return CReprocess
	default:
		s.IfRowsEvent = false
		return CRecontinue
	}
}

var eventChan = make(chan myBinEvent, 100)

func startListenBinEvents(pos mysql.Position) {
	defer close(eventChan)
	replStreamer := newBinlogStreamer(pos)
	sendBinlogEvent(replStreamer, eventChan)
}

func newBinlogStreamer(pos mysql.Position) *replication.BinlogStreamer {
	replCfg := replication.BinlogSyncerConfig{
		ServerID:                1113306,
		Flavor:                  "mysql",
		Host:                    confCmd.Host,
		Port:                    uint16(confCmd.Port),
		User:                    confCmd.User,
		Password:                confCmd.Passwd,
		Charset:                 "utf8",
		SemiSyncEnabled:         false,
		TimestampStringLocation: confCmd.BinlogTimeLocation,
		ParseTime:               false, //donot parse mysql datetime/time column into go time structure, take it as string
		UseDecimal:              false, // sqlbuilder not support decimal type
	}

	replSyncer := replication.NewBinlogSyncer(replCfg)

	replStreamer, err := replSyncer.StartSync(pos)
	if err != nil {
		log.Fatalf("error replication from master %s:%d ", confCmd.Host, confCmd.Port)
	}
	return replStreamer
}

func sendBinlogEvent(streamer *replication.BinlogStreamer, eventChan chan myBinEvent) {
	log.Println("start to get binlog from mysql")

	var (
		chkRe         int
		currentBinlog = confCmd.StartFile
		sqlType       SQLType
		tbMapPos      uint32 = 0
	)

	for {
		ev, err := streamer.GetEvent(context.Background())
		if err != nil {
			log.Fatalf("error to get binlog event, err=%s", err)
		}

		if ev.Header.EventType == replication.TABLE_MAP_EVENT {
			tbMapPos = ev.Header.LogPos - ev.Header.EventSize // avoid mysqlbing mask the row event as unknown table row event
		}

		ev.RawData = []byte{} // remove useless info
		oneMyEvent := &myBinEvent{MyPos: mysql.Position{Name: currentBinlog, Pos: ev.Header.LogPos}, StartPos: tbMapPos}

		chkRe = oneMyEvent.checkBinEvent(confCmd, ev, &currentBinlog)
		if chkRe == CRecontinue || chkRe == CRefileend {
			continue
		}

		if chkRe == CReprocess {
			sqlType = getSqlType(ev)
			if oneMyEvent.IfRowsEvent {
				tbKey := getTableName(string(oneMyEvent.BinEvent.Table.Schema), string(oneMyEvent.BinEvent.Table.Table))
				if shouldSkipTable(tbKey) {
					log.Printf("skipping binlog event for table %v", tbKey)
					continue
				}
				if _, ok := tableinfo.tableInfos[tbKey]; !ok {
					log.Printf("no table struct found for %s, it maybe dropped, skip it. RowsEvent position:%s", tbKey, oneMyEvent.MyPos.String())
					continue
				}
			}

			oneMyEvent.SqlType = sqlType
			eventChan <- *oneMyEvent
		} else {
			log.Printf("this should not happen: return value of CheckBinEvent() is %d\n", chkRe)
		}
	}
}

var eventTosqlType = map[replication.EventType]SQLType{
	replication.WRITE_ROWS_EVENTv1:  SQLTypeInsert,
	replication.WRITE_ROWS_EVENTv2:  SQLTypeInsert,
	replication.UPDATE_ROWS_EVENTv1: SQLTypeUpdate,
	replication.UPDATE_ROWS_EVENTv2: SQLTypeUpdate,
	replication.DELETE_ROWS_EVENTv1: SQLTypeDelete,
	replication.DELETE_ROWS_EVENTv2: SQLTypeDelete,
	replication.QUERY_EVENT:         SQLTypeQuery,
	replication.MARIADB_GTID_EVENT:  SQLTypeQuery,
	replication.XID_EVENT:           SQLTypeQuery,
}

func getSqlType(ev *replication.BinlogEvent) SQLType {
	if t, ok := eventTosqlType[ev.Header.EventType]; ok {
		return t
	}
	return SQLTypeQuery
}
