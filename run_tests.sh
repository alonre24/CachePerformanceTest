#!/bin/bash

for hit_rate in 0.0 0.5 0.75 0.9 1.0

do
	redis-cli flushdb
	go run performance.go -hit_rate $hit_rate -total_commands $2

done

