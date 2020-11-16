# RedisGraph Cache Benchmark

This repo aims to benchmark RedisGraph caching mechanism in terms of throughput and memory consumpation.

For running this benchmark, use the command:
```
go run performance.go [-host host] [-total_commands total_commands] [-clients clients] [-hit_rate hit_rate]
```
while RedisGraph is running in the background with CACHE_SIZE is 25.

**_argumetns:_**

**host:** redis host address (default is "127.0.0.1:6379")
**total_commands:** Number of commands to execute (default is 1000000).
**clients:** Number of concurrent clients. The commands are split equally between the clients.
**hit_rate:** Expected cache hit rate.

Output includes Average throughput (in commands per second) and server's memory usage (in MB), along with the actual hit rate and AvgServerLatency.csv file which indicates the average latency of the server (in NanoSeconds) every 10 MiliSeoncds.  



