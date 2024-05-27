import sqlite3
import time
from gspj import *
from gldj import *
from brutal_force import *
from utils import *
import csv
from multiprocessing import Process, Queue

class TimeoutException(Exception):
    pass

def run_query(query, queue):
    try:
        conn = sqlite3.connect('../imdb.db')
        c = conn.cursor()
        c.execute(query)
        result = c.fetchone()[0]
        conn.commit()
        conn.close()
        queue.put(result)
    except Exception as e:
        queue.put('Error: {}'.format(e))
        if conn:
            conn.rollback()
            conn.close()

def execute_a_query(query, timeout=500):
    """
    Input: A Query and optional timeout in seconds (default 5 seconds)
    Output: Result of the query and the time taken to execute the query
    """
    TIMEOUT = False
    start_time = time.time()
    queue = Queue()
    process = Process(target=run_query, args=(query, queue))
    
    process.start()
    process.join(timeout)
    
    if process.is_alive():
        process.terminate()
        process.join()
        print('Timeout warning :\nQuery {} \nexceeded timeout of {} seconds'.format(query, timeout))
        result = 'Timeout'
        TIMEOUT = True
    else:
        result = queue.get()

    end_time = time.time()
    runtime = round(end_time - start_time, 3) if not TIMEOUT else 10000
    
    return result, runtime


if __name__ == '__main__':
    # 自己把他改成你的query list
    queries = [
        "SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx,movie_companies mc WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.id=mc.movie_id AND mi_idx.info_type_id=113 AND mi.info_type_id=105;",
        "SELECT COUNT(*) FROM movie_keyword mk,title t,cast_info ci WHERE t.id=mk.movie_id AND t.id=ci.movie_id AND t.production_year>2010 AND  mk.keyword_id=8200;"
    ]
    for query in queries:
        original_result, original_time = execute_a_query(query, 1000)
        print("The original query:")
        print("Time:", original_time)
        print("Result:", original_result, '\n')

        gspj_query = greedy_selective_pairwise_join(query)
        gspj_query_result, gspj_query_time = execute_a_query(gspj_query, original_time*3)
        print("The GSPJ query:")
        print("Time:", gspj_query_time)
        print("Result:", gspj_query_result, '\n')

        gldj_query = greedy_left_deep_join(query)
        gldj_query_result, gldj_query_time = execute_a_query(gldj_query, original_time*3)
        print("The GLDJ query:")
        print("Time:", gldj_query_time)
        print("Result:", gldj_query_result, '\n')

        all_possible_query = Brutal_Force(query)
        bf_min_time = float('inf')
        bf_min_result = None
        bf_query = None
        for i in range(len(all_possible_query)):
            query_result, query_time = execute_a_query(all_possible_query[i], original_time*3)
            if(bf_min_result == None):
                bf_min_result = query_result
                bf_query = all_possible_query[i]
            
            if(query_time < bf_min_time and query_result == bf_min_result):
                bf_min_time = query_time
                bf_query = all_possible_query[i]
        
        print("The Brutal Force query:")
        print("Time:", bf_min_time)
        print("Result:", bf_min_result, '\n')

        with open('output.csv', 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([query.replace('\n', ' ').strip(), original_result, original_time, gspj_query_time, gldj_query_time, bf_min_time, gspj_query.replace('\n', ' ').strip(), gldj_query.replace('\n', ' ').strip(), bf_query.replace('\n', ' ').strip()])