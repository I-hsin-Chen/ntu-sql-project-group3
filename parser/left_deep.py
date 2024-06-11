import re
import itertools
from sqlalchemy import create_engine, text
import random

from utils import to_cross_join, extract_tables_and_conditions, extract_related_conditions, cardinality_estimation


def left_deep_join(query):
    '''
    This function performs a greedy left deep join on the given query.
    The returned query is the final query after performing left deep join.
    '''
    
    # Extract all the tables and conditions from the query
    output_query = None
    tables, conditions = extract_tables_and_conditions(query)
    complete_conditions = query.split('WHERE')[1].split(';')[0].strip()

    cardinalities = {}
    node_pairs = {}
    
    # Loop over all pairs of tables and extract conditions related to them
    for table1, table2 in list(itertools.combinations(tables, 2)):
        other_tables = [table for table in tables if table not in (table1, table2)]
        node_pairs[(table1, table2)] = extract_related_conditions(conditions, table1, table2, other_tables)
        # print(f'Conditions related to {table1} and {table2}: {node_pairs[(table1, table2)]}')
    

    # Estimating cardinality for each table pair (node pair)
    for node_pair in node_pairs:
        table1, table2 = node_pair
        condition = ' AND '.join(node_pairs[node_pair])
        query = f'SELECT COUNT(*) FROM {table1[0]} {table1[1]}, {table2[0]} {table2[1]} WHERE {condition};' if condition \
            else f'SELECT COUNT(*) FROM {table1[0]} {table1[1]}, {table2[0]} {table2[1]};'
        # for testing purposes, generate random cardinalities temporarily
        cardinalities[(table1, table2)] = cardinality_estimation(query)
    
    best_combination = min(cardinalities, key=cardinalities.get)
    # print(f'Best join is between {best_combination[0]} and {best_combination[1]} with cardinality {cardinalities[best_combination]}')
    
    table1, table2 = best_combination
    match_condition = ' AND '.join(node_pairs[best_combination])
    subquery = f'{table1[0]} {table1[1]} CROSS JOIN {table2[0]} {table2[1]} WHERE {match_condition}' \
        if match_condition else f'{table1[0]} {table1[1]} CROSS JOIN {table2[0]} {table2[1]}'
    combined_alias = f"{table1[1]}_{table2[1]}"
    
    new_table = (subquery, combined_alias)
    tables.remove(table1)
    tables.remove(table2)
    
    output_query = f'SELECT COUNT(*) FROM {table1[0]} {table1[1]} CROSS JOIN {table2[0]} {table2[1]}'

    while True:
        if(len(tables) == 1):
            output_query += f' CROSS JOIN {tables[0][0]} {tables[0][1]} WHERE {complete_conditions};'
            return output_query
            break

        cardinalities = {}
        node_pairs = {}
        
        # Loop over all tables and extract conditions related to them
        for table2 in tables:
            other_tables = [table for table in tables if table not in (new_table, table2)]
            node_pairs[(new_table, table2)] = extract_related_conditions(conditions, new_table, table2, other_tables)
            # print(f'Conditions related to {table1} and {table2}: {node_pairs[(table1, table2)]}')
    
        # Estimating cardinality for each table pair (node pair)
        for node_pair in node_pairs:
            table1, table2 = node_pair
            condition = ' AND '.join(node_pairs[node_pair])
            
            table1_sp = table1[0].split(' WHERE ')
            query = f'SELECT COUNT(*) FROM {table1_sp[0]} CROSS JOIN {table2[0]} {table2[1]} WHERE {condition};' if condition \
                else f'SELECT COUNT(*) FROM {table1_sp[0]} CROSS JOIN {table2[0]} {table2[1]};'
            # for testing purposes, generate random cardinalities temporarily
            cardinalities[(table1, table2)] = cardinality_estimation(query)
        
        
        best_combination = min(cardinalities, key=cardinalities.get)
        # print(f'Best join is between {best_combination[0]} and {best_combination[1]} with cardinality {cardinalities[best_combination]}')
        
        table1, table2 = best_combination
        match_condition = ' AND '.join(node_pairs[best_combination])
        subquery = f'{table1[0]} CROSS JOIN {table2[0]} {table2[1]} WHERE {match_condition}' \
        if match_condition else f'{table1[0]} CROSS JOIN {table2[0]} {table2[1]}'
        combined_alias = f"{table1[1]}_{table2[1]}"
        
        new_table = (subquery, combined_alias)
        tables.remove(table2)
        
        output_query += f' CROSS JOIN {table2[0]} {table2[1]}'
        
            


if __name__ == '__main__':
    query = """
    SELECT COUNT(*) FROM movie_keyword mk,title t,cast_info ci WHERE t.id=mk.movie_id AND t.id=ci.movie_id AND t.production_year>2010 AND mk.keyword_id=8200;
    """
    print("The original query is :" + to_cross_join(query))
    left_deep_query =  left_deep_join(query)
    print("The left deep query is :\n" + left_deep_query)
