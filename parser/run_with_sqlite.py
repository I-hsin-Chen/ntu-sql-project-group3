import sqlite3
import time
from gspj import *
from gldj import *
from brutal_force import *
from utils import *
import csv

def execute_a_query(query):
    """
    Input: A Query
    Output: Result of the query and the time taken to execute the query
    """
    conn = sqlite3.connect('../imdb.db')
    c = conn.cursor()

    start_time = time.time()
    c.execute(query)
    count = c.fetchone()[0]
    conn.commit()
    end_time = time.time()
    conn.close()

    return int(count), round(end_time - start_time, 3)


if __name__ == '__main__':
    # 自己把他改成你的query list
    queries = [
        """
        SELECT COUNT(*) FROM movie_keyword mk,title t,cast_info ci WHERE t.id=mk.movie_id AND t.id=ci.movie_id AND t.production_year>2010 AND mk.keyword_id=8200;
        """
    ]

    queries = [
        "SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx,movie_companies mc WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.id=mc.movie_id AND mi_idx.info_type_id=113 AND mi.info_type_id=105;"
    ]

    for query in queries:
        # original_result, original_time = execute_a_query(query)
        # print("The original query:")
        # print("Time:", original_time)
        # print("Result:", original_result, '\n')

        # gspj_query = greedy_selective_pairwise_join(query)
        # gspj_query_result, gspj_query_time = execute_a_query(gspj_query)
        # print("The GSPJ query:")
        # print("Time:", gspj_query_time)
        # print("Result:", gspj_query_result, '\n')

        gldj_query = greedy_left_deep_join(query)
        print(gldj_query)
        gldj_query_result, gldj_query_time = execute_a_query(gldj_query)
        print("The GLDJ query:")
        print("Time:", gldj_query_time)
        print("Result:", gldj_query_result, '\n')
        exit()

        all_possible_query = Brutal_Force(query)
        bf_min_time = float('inf')
        bf_min_result = None
        bf_query = None
        for i in range(len(all_possible_query)):
            query_result, query_time = execute_a_query(all_possible_query[i])
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