# Mysqlbinlog

mysqlbinlog is a go library that can rollback your DB with little overhead 


## Why we need this
In some scenarios, say regression test, there are many cases that will change the DB data.
To make these cases independent, we need to refresh the DB after each case finishes in case it may affect next case.

Instead of dumping or ingesting the whole DB, mysqlbinlog provides a fast, delta-based way to refresh the DB. It will

- Listen and collect DB changes from mysql binlog in the background
- Generate rollback SQLs according to these changes
- Rollback DB by these SQLs

## How to get started

It is easy to use since there are only 3 functions you need to care
```
mysqlbinlog.Start()
mysqlbinlog.Rollback()
mysqlbinlog.Stop()
```

Here is an example
```
// start to listen mysql binlog
err := mysqlbinlog.Start(host, port, user, pwd, delay)
if err != nil {
    log.Fatalf("Error to listen mysql binlog, err=%v", err)
}

// run case 0 - 9
for i := 0; i < 10; i++ {
    // INSERT, DELETE, UPDATE
    time.Sleep(time.Second * 2)

    // rollback
    mysqlbinlog.Rollback()
}

// stop listening before exit
mysqlbinlog.Stop()
```
Full code example, please refer to [Example](./example/main.go)

## Note
- This library depends on the mysql binlog functionality, to use this library, please make sure the binlog has been enabled.
- The binlog events may delay depends on the actual environment, so we added `delay` parameter in `mysqlbinlog.Start()` method. If we set `delay` to 3 seconds, then we will do rollback when it passed 3 seconds since the last event.
- DDL change is not supported for now.  