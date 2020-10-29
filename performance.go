package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/redislabs/redisgraph-go"
	"strconv"
	"sync"
	"time"
)


func run_query(graph redisgraph.Graph, q string, wg *sync.WaitGroup) {

	graph.Query(q)
	// Flush graph to DB.
	_, err := graph.Commit()
	if err != nil {
		panic(err)
	}
}

func main() {

	clients_num := 10
	graph_id := "g"
	var graphs[10] redisgraph.Graph
	
	for i:=0; i<clients_num; i++ {
	
		conn, _ := redis.Dial("tcp", "0.0.0.0:6379")
		conn.Do("FLUSHALL")
		graph := redisgraph.GraphNew(graph_id, conn)
		graphs[i] = graph
	}

	wg := sync.WaitGroup{}
	start := time.Now()
	queries_num := 100
	for id := 0; id < queries_num; id++ {
		id_str := strconv.Itoa(id)
		q := "CREATE(:n {ID:" + id_str + "})"
		wg.Add(1)
		go run_query(graphs[id%10], q, &wg)
	}
	wg.Wait()
	end := time.Now()
	took := end.Sub(start)
	fmt.Println("total duration:%", took)
	for i:=0; i<clients_num; i++ {
		graphs[i].Conn.Close()
	}
}
