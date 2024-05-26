import re
import itertools
from sqlalchemy import create_engine, text
import random

from utils import to_cross_join, extract_tables_and_conditions, extract_related_conditions, cardinality_estimation


def greedy_left_deep_join(query):
    '''
    This function performs a greedy left deep join (GLDJ) on the given query.
    The returned query is the final query after performing GLDJ.
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
        query = f'SELECT COUNT(*) FROM {table1[0]} {table1[1]}, {table2[0]} {table2[1]} WHERE {condition};'
        
        # for testing purposes, generate random cardinalities temporarily
        cardinalities[(table1, table2)] = cardinality_estimation(query)
    
    best_combination = min(cardinalities, key=cardinalities.get)
    # print(f'Best join is between {best_combination[0]} and {best_combination[1]} with cardinality {cardinalities[best_combination]}')
    
    table1, table2 = best_combination
    match_condition = ' AND '.join(node_pairs[best_combination])
    subquery = f'(SELECT * FROM {table1[0]} {table1[1]}, {table2[0]} {table2[1]} WHERE {match_condition})' \
        if match_condition else f'(SELECT * FROM {table1[0]} {table1[1]}, {table2[0]} {table2[1]})'
    combined_alias = f"{table1[1]}_{table2[1]}"
    
    new_table = (subquery, combined_alias)
    tables.remove(table1)
    tables.remove(table2)

    # Remove the conditions that are already used in the best combination
    # Update the table alias in the remaining conditions
    conditions = [c for c in conditions if c not in node_pairs[best_combination]]
    for i, condition in enumerate(conditions):
        match_t1 = re.search(r'{}\.'.format(table1[1]), condition, re.IGNORECASE)
        match_t2 = re.search(r'{}\.'.format(table2[1]), condition, re.IGNORECASE)
        if match_t1:
            conditions[i] = re.sub(r'{}\.(\w+)'.format(table1[1]), f'{combined_alias}.\\1', conditions[i])
        if match_t2:
            conditions[i] = re.sub(r'{}\.(\w+)'.format(table2[1]), f'{combined_alias}.\\1', conditions[i])
    
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
            query = f'SELECT COUNT(*) FROM {table1[0]} {table1[1]}, {table2[0]} {table2[1]} WHERE {condition};'
            
            # for testing purposes, generate random cardinalities temporarily
            cardinalities[(table1, table2)] = cardinality_estimation(query)
        
        
        best_combination = min(cardinalities, key=cardinalities.get)
        # print(f'Best join is between {best_combination[0]} and {best_combination[1]} with cardinality {cardinalities[best_combination]}')
        
        table1, table2 = best_combination
        match_condition = ' AND '.join(node_pairs[best_combination])
        subquery = f'(SELECT * FROM {table1[0]} {table1[1]}, {table2[0]} {table2[1]} WHERE {match_condition})' \
            if match_condition else f'(SELECT * FROM {table1[0]} {table1[1]}, {table2[0]} {table2[1]})'
        combined_alias = f"{table1[1]}_{table2[1]}"
        
        new_table = (subquery, combined_alias)
        tables.remove(table2)
        
        # Remove the conditions that are already used in the best combination
        # Update the table alias in the remaining conditions
        conditions = [c for c in conditions if c not in node_pairs[best_combination]]
        for i, condition in enumerate(conditions):
            match_t1 = re.search(r'{}\.'.format(table1[1]), condition, re.IGNORECASE)
            match_t2 = re.search(r'{}\.'.format(table2[1]), condition, re.IGNORECASE)
            if match_t1:
                conditions[i] = re.sub(r'{}\.(\w+)'.format(table1[1]), f'{combined_alias}.\\1', conditions[i])
            if match_t2:
                conditions[i] = re.sub(r'{}\.(\w+)'.format(table2[1]), f'{combined_alias}.\\1', conditions[i])
    
        output_query += f' CROSS JOIN {table2[0]} {table2[1]}'
        
            


if __name__ == '__main__':
    query = """
    SELECT COUNT(*) FROM movie_keyword mk,title t,cast_info ci WHERE t.id=mk.movie_id AND t.id=ci.movie_id AND t.production_year>2010 AND mk.keyword_id=8200;
    """
    print("The original query is :" + to_cross_join(query))
    gldj_query = greedy_left_deep_join(query)
    print("The GLDJ query is :\n" + gldj_query)
