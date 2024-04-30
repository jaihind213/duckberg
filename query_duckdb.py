import os
import sys
import time

import duckdb

from queries import query_map

file_db_name = "file.db"

def query_using_duck(query_id: str, engine_type: str = "memory", temp_dir: str = "/tmp/duckberg", memory_gb:str = '10G'):
    """
    :param query_id: refer to map in queries.py for query_id
    :param engine_type: memory or file default to memory
    :param temp_dir: default to /tmp/duckberg
    :param memory_gb: default to 9G
    :return: time_taken_ms
    """
    connection = None
    if "file" == engine_type:
        if os.path.exists(file_db_name):
            os.remove(file_db_name)
        connection = duckdb.connect(file_db_name)
    else:
        connection = duckdb.connect(":memory:")
    connection.sql("SET memory_limit='" + memory_gb + "';")
    try:
        connection.execute(f"SET temp_directory = '{temp_dir}';")
        query  = query_map[query_id]
        print(query)
        start = time.time_ns()
        result = connection.execute(query)
        print(result.fetchall())
        end = time.time_ns()
        query_time_ms = (end-start)/1e6
        return query_time_ms
    finally:
        if connection is not None:
            connection.close()

if __name__ == "__main__":
    query_id = sys.argv[1]
    tmp_dir = "/tmp/duckberg"
    mem = "9G"
    engine = "memory"
    if len(sys.argv) > 2:
        mem = sys.argv[2]
    if len(sys.argv) > 3:
        tmp_dir = sys.argv[3]
    if len(sys.argv) > 4:
        engine = sys.argv[4]
    query_time_ms = query_using_duck(query_id, engine, temp_dir=tmp_dir, memory_gb=mem)

    file_path = "results_duck.csv"
    if not os.path.exists(file_path):
        with open(file_path, 'w') as file:
            file.write("query_id,engine,query_time_ms")
    with open(file_path, 'a') as fd:
        fd.write(f'\n{query_id},duckdb,{query_time_ms}')

