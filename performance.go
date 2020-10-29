package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/mediocregopher/radix/v3"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var totalCommands uint64

type testResult struct {
	StartTime      int64   `json:"StartTime"`
	Duration       float64 `json:"Duration"`
	Pipeline       int     `json:"Pipeline"`
	CommandRate    float64 `json:"CommandRate"`
	TotalNodes     uint64  `json:"TotalNodes"`
	NodeMin        uint64  `json:"NodeMin"`
	NodeMax        uint64  `json:"NodeMax"`
	P50IngestionMs float64 `json:"p50IngestionMs"`
	P95IngestionMs float64 `json:"p95IngestionMs"`
	P99IngestionMs float64 `json:"p99IngestionMs"`
}

// to be called as go routine.
// creates a connection, sets the client name and executes graph.query <graph.name> "CREATE(:n {ID:<node.id>})"
// - where node.id is between [start_node,end_node[
// calculated latencies in microseconds are sent back via channel
// pipelining of commands is supported
// unix sockets are supported
func graphNodesRoutine(addr, socket string, pipelineSize int, clientName, graphName string, start_node, end_node int, wg *sync.WaitGroup, latencyChan chan int64) {
	defer wg.Done()
	var conn radix.Conn
	var err error
	if socket != "" {
		conn, err = radix.Dial("unix", socket)
	} else {
		conn, err = radix.Dial("tcp", addr)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	err = conn.Do(radix.FlatCmd(nil, "CLIENT", "SETNAME", clientName))
	if err != nil {
		log.Fatal(err)
	}

	currentPipelineSize := 0
	cmds := make([]radix.CmdAction, 0)
	times := make([]time.Time, 0)
	for i := start_node; i < end_node; i++ {
		cypher := fmt.Sprintf("CREATE(:n {ID:%d})", i)
		cmd := radix.FlatCmd(nil, "GRAPH.QUERY", graphName, cypher)
		cmds = append(cmds, cmd)
		currentPipelineSize++
		startT := time.Now()
		times = append(times, startT)
		if currentPipelineSize >= pipelineSize {
			err = conn.Do(radix.Pipeline(cmds...))
			endT := time.Now()
			if err != nil {
				log.Fatal(err)
			}
			for _, t := range times {
				duration := endT.Sub(t)
				latencyChan <- int64(duration.Microseconds())
			}
			atomic.AddUint64(&totalCommands, uint64(pipelineSize))
			cmds = make([]radix.CmdAction, 0)
			times = make([]time.Time, 0)
			currentPipelineSize = 0
		}
	}
}

func main() {
	host := flag.String("host", "127.0.0.1:6379", "redis host.")
	socket := flag.String("socket", "", "redis socket. overides --host.")
	pipeline := flag.Int("pipeline", 1, "pipeline.")
	node_minimum := flag.Int("node-minimum", 1, "node ID minimum value.")
	node_maximum := flag.Int("node-maximum", 1000000, "node ID maximum value.")
	clients := flag.Int("clients", 50, "number of concurrent clients.")
	json_out_file := flag.String("json-out-file", "", "Name of json output file, if not set, will not print to json.")
	flag.Parse()

	// listen for C-c
	c := make(chan os.Signal, 1)
	latencyChan := make(chan int64, 1)

	signal.Notify(c, os.Interrupt)

	total_nodes := *node_maximum - *node_minimum + 1
	commands_per_client := total_nodes / *clients
	fmt.Printf("Total nodes: %d. Total clients: %d. Inserted nodes per client %d.\n", total_nodes, *clients, commands_per_client)
	fmt.Printf("Pipeline: %d\n", *pipeline)

	latencies := hdrhistogram.New(1, 90000000, 3)

	// a WaitGroup for the goroutines to tell us they've stopped
	wg := sync.WaitGroup{}
	statsWg := sync.WaitGroup{}
	statsWg.Add(1)

	tick := time.NewTicker(time.Second)

	for client_id := 1; client_id <= *clients; client_id++ {
		clientName := fmt.Sprintf("client#%d", *clients)
		wg.Add(1)
		node_start := *node_minimum + ((client_id - 1) * commands_per_client)
		node_end := *node_minimum + ((client_id) * commands_per_client)
		go graphNodesRoutine(*host, *socket, *pipeline, clientName, "g", node_start, node_end, &wg, latencyChan)
	}
	go recordLatencies(latencyChan, latencies, &statsWg)
	closed, start_time, duration, totalCommands, _ := updateCLI(tick, c, uint64(total_nodes), latencies)
	if !closed {
		wg.Wait()
		statsWg.Wait()
	}

	commandRate := float64(totalCommands) / float64(duration.Seconds())
	p50IngestionMs := float64(latencies.ValueAtQuantile(50.0)) / 1000.0
	p95IngestionMs := float64(latencies.ValueAtQuantile(95.0)) / 1000.0
	p99IngestionMs := float64(latencies.ValueAtQuantile(99.0)) / 1000.0

	fmt.Fprint(os.Stdout, "\n")
	fmt.Fprint(os.Stdout, "#################################################\n")
	fmt.Fprintf(os.Stdout, "Total Duration %.3f Seconds\n", duration.Seconds())
	fmt.Fprintf(os.Stdout, "Total Nodes created %d\n", totalCommands)
	fmt.Fprintf(os.Stdout, "Throughput summary: %.0f requests per second\n", commandRate)
	fmt.Fprintf(os.Stdout, "Latency summary (msec):\n")
	fmt.Fprintf(os.Stdout, "    %9s %9s %9s\n", "p50", "p95", "p99")
	fmt.Fprintf(os.Stdout, "    %9.3f %9.3f %9.3f\n", p50IngestionMs, p95IngestionMs, p99IngestionMs)

	if strings.Compare(*json_out_file, "") != 0 {

		res := testResult{
			StartTime:      start_time.Unix(),
			Pipeline:       *pipeline,
			Duration:       duration.Seconds(),
			CommandRate:    commandRate,
			TotalNodes:     totalCommands,
			NodeMin:        uint64(*node_minimum),
			NodeMax:        uint64(*node_maximum),
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
func updateCLI(tick *time.Ticker, c chan os.Signal, message_limit uint64, latencies *hdrhistogram.Histogram) (bool, time.Time, time.Duration, uint64, []float64) {

	start := time.Now()
	prevTime := time.Now()
	prevMessageCount := uint64(0)
	commandRateTs := []float64{}
	fmt.Printf("%26s %7s %25s %25s %25s\n", "Test time", " ", "Total Commands", "Command Rate", "p50 lat. (msec)")
	for {
		select {
		case <-tick.C:
			{
				now := time.Now()
				took := now.Sub(prevTime)
				commandRate := float64(totalCommands-prevMessageCount) / float64(took.Seconds())
				completionPercent := float64(totalCommands) / float64(message_limit) * 100.0

				p50 := float64(latencies.ValueAtQuantile(50.0)) / 1000.0

				if prevMessageCount == 0 && totalCommands != 0 {
					start = time.Now()
				}
				if totalCommands != 0 {
					commandRateTs = append(commandRateTs, commandRate)
				}
				prevMessageCount = totalCommands
				prevTime = now

				fmt.Printf("%25.0fs [%3.1f%%] %25d %25.2f %25.2f\t", time.Since(start).Seconds(), completionPercent, totalCommands, commandRate, p50)
				fmt.Fprint(os.Stdout, "\r")
				if totalCommands >= uint64(message_limit) {
					return true, start, time.Since(start), totalCommands, commandRateTs
				}

				break
			}

		case <-c:
			fmt.Println("received Ctrl-c - shutting down")
			return true, start, time.Since(start), totalCommands, commandRateTs
		}
	}
}
