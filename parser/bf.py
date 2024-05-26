import re
import itertools
from sqlalchemy import create_engine, text


# 假設這些函數是從 utils 模塊中引入的
from utils import extract_tables_and_conditions, extract_related_conditions, cardinality_estimation

def generate_all_possible_queries(tables, conditions):
    """
    Generate all possible SQL queries given tables and conditions.
    """
    permutations = itertools.permutations(tables)
    possible_queries = []

    for perm in permutations:
        query_parts = [f'SELECT COUNT(*) FROM {perm[0][0]} {perm[0][1]}']
        for i in range(1, len(perm)):
            join_conditions = extract_related_conditions(conditions, perm[i-1], perm[i], perm[:i-1] + perm[i+1:])
            join_clause = f' CROSS JOIN {perm[i][0]} {perm[i][1]}'
            if join_conditions:
                join_clause += f' ON {" AND ".join(join_conditions)}'
            query_parts.append(join_clause)
        query_parts.append(f' WHERE {" AND ".join(conditions)};')
        possible_queries.append("".join(query_parts))

    return possible_queries

def bf(query):
    """
    This function performs a brute force join on the given query.
    The returned query is the final query after performing the brute force join.
    """
    tables, conditions = extract_tables_and_conditions(query)
    possible_queries = generate_all_possible_queries(tables, conditions)

    best_query = None
    min_cardinality = float('inf')

    for q in possible_queries:
        cardinality = cardinality_estimation(q)
        if cardinality < min_cardinality:
            min_cardinality = cardinality
            best_query = q

    return best_query

if __name__ == '__main__':
    query = """
    SELECT COUNT(*) 
    FROM 
    movie_companies mc, title t, movie_keyword mk 
    WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mk.keyword_id=117;
    """
    bf_query = bf(query)
    print("BF Query:", bf_query)
