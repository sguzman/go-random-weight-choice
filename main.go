package main

import (
    "database/sql"
    "fmt"
    "github.com/deckarep/golang-set"
    "github.com/jmcvetta/randutil"
    _ "github.com/lib/pq"
    "os"
    "runtime"
    "time"
)

type Data struct {
    serial string
    subs   uint64
}

const (
    defaultHost = "192.168.1.63"
    defaultPort = "30000"
)

var (
    choices []randutil.Choice
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
        serials = append(serials, choice)
    }

    return serials
}

func choose(chs []randutil.Choice) randutil.Choice {
    ch, err := randutil.WeightedChoice(chs)
    if err != nil {
        panic(err)
    }

    return ch
}

func chooseN(chs []randutil.Choice, n int) []string {
    set := mapset.NewSet()

    for set.Cardinality() < n {
        set.Add(choose(chs).Item.(string))
    }

    array := make([]string, n)
    arraySet := set.ToSlice()
    for i := range array {
        array[i] = arraySet[i].(string)
    }

    return array
}

func main() {
    go func() {
        for {
            choices = channels()
        }
    }()

    go func() {
        for {
            time.Sleep(100 * time.Second)
            runtime.GC()
        }
    }()
}
