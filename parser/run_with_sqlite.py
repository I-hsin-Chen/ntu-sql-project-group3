import sqlite3
import time
from gspj import *
from utils import *

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

    return count, end_time - start_time


if __name__ == '__main__':
    query = """
    SELECT COUNT(*) FROM movie_keyword mk,title t,cast_info ci WHERE t.id=mk.movie_id AND t.id=ci.movie_id AND t.production_year>2010 AND mk.keyword_id=8200;
    """
    
    original_result, original_time = execute_a_query(query)
    print(original_result, original_time)

    gspj_query = greedy_selective_pairwise_join(query)
    gspj_query_result, gspj_query_time = execute_a_query(gspj_query)
    print(gspj_query_result, gspj_query_time)

    