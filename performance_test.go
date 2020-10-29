package main

import (
	"redisgraph"
	"time"
	"sync"
	"github.com/gomodule/redigo/redis"
)

var graph Graph

func createGraph() {
	conn, _ := redis.Dial("tcp", "0.0.0.0:6379")
	conn.Do("FLUSHALL")
	graph = GraphNew("social", conn)

	// Flush graph to DB.
	_, err := graph.Commit()
	if err != nil {
		panic(err)
	}
}

func run_query (q string) {
	
	defer wg.done()
	res, err := graph.Query(q)
	// Flush graph to DB.
	_, err = graph.Commit()
	if err != nil {
		panic(err)
	}
	
}

func main () {

	createGraph()
	wg := sync.WaitGruop{}
	start := time.Now()
	c = make(chan int)
	queries_num := 1000000
	for id:=0; id<queries_num; id++ {
		q := "Create (n: 'ID':" + i + ")"
		wg.Add(1)
		go run_query(q)
	}
	wg.wait() 
	end := time.Now()
	fmt.Println("total duration: %f", end-start)
	graph.Conn.Close()
}



