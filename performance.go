package main

import (
	"encoding/json"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/gomodule/redigo/redis"
	"github.com/RedisGraph/redisgraph-go"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"math/rand"
	"strconv"
)

var totalCommandsDone uint64
var totalCachedCommands uint64
var totalServerLatancies uint64

type testResult struct {
	StartTime      int64   `json:"StartTime"`
	Duration       float64 `json:"Duration"`
	Pipeline       int     `json:"Pipeline"`
	CommandRate    float64 `json:"CommandRate"`
	TotalCommands  uint64  `json:"TotalNodes"`
	P50IngestionMs float64 `json:"p50IngestionMs"`
	P95IngestionMs float64 `json:"p95IngestionMs"`
	P99IngestionMs float64 `json:"p99IngestionMs"`
}

// to be called as go routine.
// creates a connection, sets the client name and executes graph.query commands"
// calculated latencies in microseconds are sent back via channel
// unix sockets are supported
func graphNodesRoutine(addr, socket string, pipelineSize int, clientName, graphName string, wg *sync.WaitGroup, latencyChan chan int64, client_id, commandes_per_client, clients int, hit_rate float64) {
	defer wg.Done()
	var conn redis.Conn
	var err error
	conn, err = redis.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	
	defer conn.Close()
	graph := redisgraph.GraphNew(graphName, conn)

	times := make([]time.Time, 0)
	curr_insert := client_id
	reccurent_queries_num := int(25 * hit_rate)
	for i := 1; i <= commandes_per_client; i++ {
		rand_num := rand.Float64()
		cypher := ""
		if rand_num < hit_rate {
			// To acheive cache hit with high probability, we execute one of the reccurent queries.
			hit_id := rand.Intn(reccurent_queries_num)
			cypher = fmt.Sprintf("MATCH (a:person {ID_hit:%d})-[:friend|:visited*]->(e) RETURN e.name, count(e.name) AS NumPathsToEntity ORDER BY NumPathsToEntity, e.name DESC", hit_id)
			//cypher = fmt.Sprintf("MATCH (a:l{ID_hit:%d}) RETURN a", hit_id)
		} else {
			// To acheive cache miss, we execute a new command
			curr_insert += clients
			cypher = fmt.Sprintf("MATCH (a:person {ID:%d})-[:friend|:visited*]->(e) RETURN e.name, count(e.name) AS NumPathsToEntity ORDER BY NumPathsToEntity, e.name DESC", curr_insert)
			//cypher = fmt.Sprintf("MATCH (a:l{ID:%d}) RETURN a", curr_insert)
		}
		
		startT := time.Now()
		times = append(times, startT)
		res, err := graph.Query(cypher)
		endT := time.Now()
		if err != nil {
			log.Fatal(err)
		}
		for _, t := range times {
			duration := endT.Sub(t)
			latencyChan <- int64(duration.Microseconds())
		}
		atomic.AddUint64(&totalCommandsDone, uint64(1))
		atomic.AddUint64(&totalServerLatancies, uint64(res.RunTime()*1000))
		if res.CachedExecution() == 1 {
			atomic.AddUint64(&totalCachedCommands, uint64(1))
		}
		times = make([]time.Time, 0)
		
	}
}

func main() {

	host := flag.String("host", "127.0.0.1:6379", "redis host.")
	socket := flag.String("socket", "", "redis socket. overides --host.")
	pipeline := flag.Int("pipeline", 1, "pipeline.")
	total_commands := flag.Int("total_commands", 1000000, "number of commands to execute.")
	clients := flag.Int("clients", 20, "number of concurrent clients.")
	hit_rate := flag.Float64("hit_rate", 0.9, "probability to get a cache hit.")
	json_out_file := flag.String("json-out-file", "", "Name of json output file, if not set, will not print to json.")
	flag.Parse()
	
	// listen for C-c
	c := make(chan os.Signal, 1)
	latencyChan := make(chan int64, 1)

	signal.Notify(c, os.Interrupt)

	commands_per_client := *total_commands / *clients
	fmt.Printf("Total commandes: %d. Total clients: %d. Commands per client %d.\n", *total_commands, *clients, commands_per_client)
	fmt.Printf("Pipeline: %d\n", *pipeline)

	latencies := hdrhistogram.New(1, 90000000, 3)

	// a WaitGroup for the goroutines to tell us they've stopped
	wg := sync.WaitGroup{}
	statsWg := sync.WaitGroup{}
	statsWg.Add(1)

	tick := time.NewTicker(10 * time.Millisecond)
	//tick := time.NewTicker(time.Second)

	for client_id := 1; client_id <= *clients; client_id++ {
		clientName := fmt.Sprintf("client#%d", *clients)
		wg.Add(1)
		go graphNodesRoutine(*host, *socket, *pipeline, clientName, "g", &wg, latencyChan, client_id, commands_per_client, *clients, *hit_rate)
	}
	go recordLatencies(latencyChan, latencies, &statsWg)
	closed, start_time, duration, totalCommandsDone, serverLatencies, memoryUsage := updateCLI(tick, c, uint64(*total_commands), latencies)
	if !closed {
		wg.Wait()
		statsWg.Wait()
	}

	commandRate := float64(totalCommandsDone) / float64(duration.Seconds())
	p50IngestionMs := float64(latencies.ValueAtQuantile(50.0)) / 1000.0
	p95IngestionMs := float64(latencies.ValueAtQuantile(95.0)) / 1000.0
	p99IngestionMs := float64(latencies.ValueAtQuantile(99.0)) / 1000.0
	//commandRateTs = commandRateTs[:len(commandRateTs)-1]
	n := len(memoryUsage)
	sum := 0
	for i := 0; i < n; i++ { 
        	sum += (memoryUsage[i]) 
    	}
    	avgMemoryUsage := (float64(sum)) / (float64(n)) 
	
	file, _ := os.Create("AvgServerLatency.csv")
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	
	for i, value := range serverLatencies {
		_ = writer.Write([]string{fmt.Sprintf("%d", i), fmt.Sprintf("%f", value)})
	}

	fmt.Fprint(os.Stdout, "\n")
	fmt.Fprint(os.Stdout, "#################################################\n")
	fmt.Fprintf(os.Stdout, "Total Duration %.3f Seconds\n", duration.Seconds())
	fmt.Fprintf(os.Stdout, "Total commands executed %d\n", totalCommandsDone)
	fmt.Fprintf(os.Stdout, "Actual cache hit rate: %.4f\n", float64(totalCachedCommands) / float64(totalCommandsDone))
	fmt.Fprintf(os.Stdout, "Mean throughput: %.4f requests per second\n", commandRate)
	fmt.Fprintf(os.Stdout, "Memory Usage: %.0f B\n", avgMemoryUsage)
	fmt.Fprintf(os.Stdout, "Latency summary (msec):\n")
	fmt.Fprintf(os.Stdout, "    %9s %9s %9s\n", "p50", "p95", "p99")
	fmt.Fprintf(os.Stdout, "    %9.3f %9.3f %9.3f\n", p50IngestionMs, p95IngestionMs, p99IngestionMs)

	if strings.Compare(*json_out_file, "") != 0 {

		res := testResult{
			StartTime:      start_time.Unix(),
			Pipeline:       *pipeline,
			Duration:       duration.Seconds(),
			CommandRate:    commandRate,
			TotalCommands:  totalCommandsDone,
			P50IngestionMs: p50IngestionMs,
			P95IngestionMs: p95IngestionMs,
			P99IngestionMs: p99IngestionMs,
		}
		file, err := json.MarshalIndent(res, "", " ")
		if err != nil {
			log.Fatal(err)
		}

		err = ioutil.WriteFile(*json_out_file, file, 0644)
		if err != nil {
			log.Fatal(err)
		}
	}
}

// go routine to collect latencies ( in non locking safe manner )
func recordLatencies(latencyChan chan int64, latencies *hdrhistogram.Histogram, wg *sync.WaitGroup) {
	defer wg.Done()
	for latency := range latencyChan {
		err := latencies.RecordValue(latency)
		if err != nil {
			log.Fatal(err)
		}
	}
}

// method called each second just to print stats
func updateCLI(tick *time.Ticker, c chan os.Signal, message_limit uint64, latencies *hdrhistogram.Histogram) (bool, time.Time, time.Duration, uint64, []float64, []int) {

	start := time.Now()
	prevTime := time.Now()
	prevMessageCount := uint64(0)
	commandRateTs := []float64{}
	memoryUsage := []int{}
	serverLatencies := []float64{}
	var conn redis.Conn
	var err error
	conn, err = redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	
	fmt.Printf("%26s %7s %25s %25s %25s\n", "Test time", " ", "Total Commands", "Command Rate", "p50 lat. (msec)")
	for {
		select {
		case <-tick.C:
			{
				now := time.Now()
				took := now.Sub(prevTime)
				commandRate := float64(totalCommandsDone-prevMessageCount) / float64(took.Seconds())
				completionPercent := float64(totalCommandsDone) / float64(message_limit) * 100.0

				p50 := float64(latencies.ValueAtQuantile(50.0)) / 1000.0

				if prevMessageCount == 0 && totalCommandsDone != 0 {
					start = time.Now()
				}
				if totalCommandsDone != 0 {
					commandRateTs = append(commandRateTs, commandRate)
				} 
				serverLatencies = append(serverLatencies, float64(totalServerLatancies) / float64(totalCommandsDone-prevMessageCount))
				atomic.StoreUint64(&totalServerLatancies, 0)
				reply, err := redis.String(conn.Do("INFO", "memory"))
				if err != nil {
					log.Fatal(err)
				}
				memoryInfo := strings.Split(reply, "\r\n")[1]
				currMemoryUsageStr := strings.Split(memoryInfo, ":")[1]
				currMemoryUsage, err := strconv.Atoi(currMemoryUsageStr)
				if err != nil {
					log.Fatal(err)
				}
				memoryUsage = append(memoryUsage, currMemoryUsage)
				prevMessageCount = totalCommandsDone
				prevTime = now

				fmt.Printf("%25.0fs [%3.1f%%] %25d %25.2f %25.2f\t", time.Since(start).Seconds(), completionPercent, totalCommandsDone, commandRate, p50)
				fmt.Fprint(os.Stdout, "\r")
				if totalCommandsDone >= uint64(message_limit) {
					return true, start, time.Since(start), totalCommandsDone, serverLatencies, memoryUsage
				}

				break
			}

		case <-c:
			fmt.Println("received Ctrl-c - shutting down")
			return true, start, time.Since(start), totalCommandsDone, serverLatencies, memoryUsage
		}
	}
}
