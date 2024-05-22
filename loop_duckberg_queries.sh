#!/bin/bash
ENGINE=memory
#ENGINE=file
for i in {1..16}; do
	python query_duckdb.py query_$i 9G /tmp/duckberg $ENGINE True
done

