package main

import (
    "database/sql"
    "fmt"
    "github.com/jmcvetta/randutil"
    _ "github.com/lib/pq"
    "os"
    "runtime"
)

type Data struct {
    serial string
    subs   uint64
}

const (
    defaultHost = "192.168.1.63"
    defaultPort = "30000"
)

func (that Data) String() string {
    return fmt.Sprintf("{%s, %d}",
        that.serial, that.subs)
}

func connStr() string {
    host := os.Getenv("DB_HOST")
    port := os.Getenv("DB_PORT")

    if len(host) == 0 || len(port) == 0 {
        return fmt.Sprintf("user=postgres dbname=youtube host=%s port=%s sslmode=disable", defaultHost, defaultPort)
    } else {
        return fmt.Sprintf("user=postgres dbname=youtube host=%s port=%s sslmode=disable", host, port)
    }
}

func connection() *sql.DB {
    db, err := sql.Open("postgres", connStr())
    if err != nil {
        panic(err)
    }

    return db
}

func channels() []randutil.Choice {
    sqlStr := "select serial, subs from youtube.entities.chan_stats where (serial, time) in (select serial, max(time) from youtube.entities.chan_stats group by serial)"
    db := connection()
    defer func() {
        err := db.Close()
        if err != nil {
            panic(err)
        }
    }()

    row, err := db.Query(sqlStr)
    if err != nil {
        panic(err)
    }

    var serials []randutil.Choice
    for row.Next() {
        var serial string
        var subs int

        err = row.Scan(&serial, &subs)
        if err != nil {
            panic(err)
        }

        choice := randutil.Choice{
            Weight: subs,
            Item:   serial,
        }
        fmt.Println(choice)

        serials = append(serials, choice)
    }

    return serials
}

func main() {
    for {
        chans := channels()
        fmt.Println(len(chans))

        runtime.GC()
    }
}
