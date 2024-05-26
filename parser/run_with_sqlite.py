import sqlite3
import time
from gspj import *
from utils import *


if __name__ == '__main__':
    query = """
    SELECT COUNT(*) FROM movie_keyword mk,title t,cast_info ci WHERE t.id=mk.movie_id AND t.id=ci.movie_id AND t.production_year>2010 AND mk.keyword_id=8200;
    """
    
    conn = sqlite3.connect('../imdb.db')
    c = conn.cursor()
    start_time = time.time()

    c.execute(query)
    count = c.fetchone()[0]
    print("Original Query's output:", count)
    conn.commit()
    end_time = time.time()
    print(f"Original Query's execution time: {end_time - start_time()}s")

    start_time = time.time()
    gspj_query = greedy_selective_pairwise_join(query)
    print("With GSPJ algorithm, the query:", gspj_query)
    c.execute(gspj_query)
    count = c.fetchone()[0]
    print(f"GSPJ Query's execution time: {end_time - start_time()}s")
    conn.commit()
    end_time = time.time()
    print(f"GSPJ Query's execution time: {end_time - start_time()}s")

    conn.close()