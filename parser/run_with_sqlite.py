import sqlite3
import time
from bushy_tree import *
from left_deep import *
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
        queue.put('Error: {} when executing query "{}"'.format(e, query))
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
        if isinstance(result, str) and result.startswith('Error'):
            print(result)

    
    end_time = time.time()
    runtime = round(end_time - start_time, 3) if not TIMEOUT else 10000
    if not TIMEOUT:
        print('Query executed successfully in {} seconds : {}'.format(runtime, query))
    
    return result, runtime


if __name__ == '__main__':
    queries = [
        "SELECT COUNT(*) FROM movie_companies mc,movie_keyword mk,movie_info_idx mi_idx,title t,movie_info mi WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.id=mc.movie_id AND mk.keyword_id<11604 AND t.kind_id=7 AND t.production_year=2004 AND mi.info_type_id=3;"
    ]
    for query in queries:
        original_result, original_time = execute_a_query(query, 1000)
        print("The original query:")
        print("Time:", original_time)
        print("Result:", original_result, '\n')

        bushy_query = bushy_join(query)
        bushy_query_result, bushy_query_time = execute_a_query(bushy_query, original_time*3)
        print("The Bushy Tree query:")
        print("Time:", bushy_query_time)
        print("Result:", bushy_query_result, '\n')

        left_deep_query = left_deep_join(query)
        left_deep_query_result, left_deep_query_time = execute_a_query(left_deep_query, original_time*3)
        print("The Left-Deep Tree query:")
        print("Time:", left_deep_query_time)
        print("Result:", left_deep_query_result, '\n')

        all_possible_query = Brutal_Force(query)
        bf_min_time = original_time + 30
        bf_min_result = None
        bf_query = None
        for i in range(len(all_possible_query)):
            query_result, query_time = execute_a_query(all_possible_query[i], bf_min_time)
            print("=======================================================")
            if(type(query_result) == str): 
                # Timeout or Error
                continue
            
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
            writer.writerow([query.replace('\n', ' ').strip(), original_result, original_time, bushy_query_time, left_deep_query_time, 0, bushy_query.replace('\n', ' ').strip(), left_deep_query.replace('\n', ' ').strip(), ""])