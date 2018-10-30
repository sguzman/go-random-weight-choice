package main

import (
    "database/sql"
    "encoding/json"
    "fmt"
    "github.com/deckarep/golang-set"
    "github.com/jmcvetta/randutil"
    _ "github.com/lib/pq"
    "net/http"
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

func choose() randutil.Choice {
    ch, err := randutil.WeightedChoice(choices)
    if err != nil {
        panic(err)
    }

    return ch
}

func chooseN(n int) []string {
    set := mapset.NewSet()

    for set.Cardinality() < n {
        set.Add(choose().Item.(string))
    }

    array := make([]string, n)
    arraySet := set.ToSlice()
    for i := range array {
        array[i] = arraySet[i].(string)
    }

    return array
}

type Json struct {
    Serials []string `json:"serials"`
}

func getJsonSerials() []byte {
    chans := chooseN(50)
    jsonStruct := Json{
        Serials: chans,
    }
    jsonObj, err := json.Marshal(jsonStruct)
    if err != nil {
        panic(err)
    }

    return jsonObj
}

func handler(w http.ResponseWriter, r *http.Request) {
    count, err := w.Write(getJsonSerials())
    if err != nil {
        panic(err)
    }

    if count != 1363 {
        panic("Bad byte count")
    }

    fmt.Println("Wrote", count, "bytes")
}

func main() {
    go func() {
        for {
            choices = channels()
            fmt.Println("Renewed channel list:", len(choices))
            time.Sleep(100 * time.Second)
        }
    }()

    go func() {
        for {
            time.Sleep(100 * time.Second)
            fmt.Println("Garbage collection")
            runtime.GC()
        }
    }()

    for len(choices) == 0 {
        fmt.Println("Waiting for init")
        time.Sleep(1 * time.Second)
    }

    http.HandleFunc("/", handler)
    panic(http.ListenAndServe("0.0.0.0:8080", nil))
}
