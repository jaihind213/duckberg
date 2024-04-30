import os
import sys
import time

import duckdb

file_db_name = "file.db"

def list_files_recursively(folder):
    file_list = []
    for root, directories, files in os.walk(folder):
        for filename in files:
            file_list.append(os.path.join(root, filename))
    return file_list

def query_using_duck(query: str, data_path: str, engine_type: str = "memory", temp_dir: str = "/tmp/duckberg", loading_table: str = "parquet_data"):
    """
    query_using_duck
    :param query:
    :param data_path:
    :param engine_type:
    :param temp_dir:
    :return: tuple of query_time_sec,load_time_sec
    """
    connection = None
    if "file" == engine_type:
        if os.path.exists(file_db_name):
            os.remove(file_db_name)
        connection = duckdb.connect(file_db_name)
    else:
        connection = duckdb.connect(":memory:")

    try:
        connection.execute(f"SET temp_directory = '{temp_dir}';")
        files = list_files_recursively(data_path)
        loading_query = f"CREATE TABLE {loading_table} AS SELECT * FROM read_parquet(" + files.__str__() + ");"
        start = time.time()
        connection.execute(loading_query)
        end = time.time()
        loading_time_sec = end-start

        start = time.time()
        result = connection.execute(query)
        print(result.fetchall())
        end = time.time()
        query_time_sec = end-start
        return (query_time_sec, loading_time_sec)
    finally:
        if connection is not None:
            connection.close()

if __name__ == "__main__":
    query = sys.argv[1]
    data_path = sys.argv[2]
    loading_table_name = None
    tmp_dir = None
    if len(sys.argv) > 3:
        loading_table_name = sys.argv[3]
    if len(sys.argv) > 4:
        tmp_dir = sys.argv[4]
    for engine_type in ("memory", "file"):
        query_time_sec, load_time_sec = query_using_duck(query, data_path, engine_type, loading_table=loading_table_name, temp_dir=tmp_dir)
        #print(f"engine_type: {engine_type}, query_time_sec: {query_time_sec}, load_time_sec: {load_time_sec}, total_sec: {query_time_sec+load_time_sec}")
        print(f"{engine_type},{query_time_sec},{load_time_sec},{query_time_sec+load_time_sec}")
