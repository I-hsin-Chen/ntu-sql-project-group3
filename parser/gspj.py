import re
import itertools
from sqlalchemy import create_engine, text

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import to_cross_join, extract_tables_and_conditions, extract_related_conditions, cardinality_estimation

def greedy_selective_pairwise_join(query):
    '''
    This function performs a greedy selective pairwise join (GSPJ) on the given query.
    The returned query is the final query after performing GSPJ.
    '''
    
    # Extract all the tables and conditions from the query
    tables, conditions = extract_tables_and_conditions(query)
    
    while True:
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
            cardinalities[(table1, table2)] = cardinality_estimation(query)
        
        best_combination = min(cardinalities, key=cardinalities.get)
        print(f'Best join is between {best_combination[0]} and {best_combination[1]} with cardinality {cardinalities[best_combination]}')
        
        table1, table2 = best_combination
        match_condition = ' AND '.join(node_pairs[best_combination])
        subquery = f'(SELECT * FROM {table1[0]} {table1[1]}, {table2[0]} {table2[1]} WHERE {match_condition})' \
            if match_condition else f'(SELECT * FROM {table1[0]} {table1[1]}, {table2[0]} {table2[1]})'
        combined_alias = f"{table1[1]}_{table2[1]}"
        
        new_table = (subquery, combined_alias)
        tables.remove(table1)
        tables.remove(table2)
        tables.append(new_table)
        
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
        
        # If there are only two tables left, then just simply join them and return the final query
        if len(tables) == 2:
            remaining_conditions = ' AND '.join(conditions)
            final_query = f'SELECT COUNT(*) FROM {tables[0][0]} {tables[0][1]}, {tables[1][0]} {tables[1][1]} WHERE {remaining_conditions};' \
                if remaining_conditions else f'SELECT COUNT(*) FROM {tables[0][0]} {tables[0][1]}, {tables[1][0]} {tables[1][1]};'
            
            final_query = to_cross_join(final_query)
            print("The final result is :\n" + final_query)
            break
        
        return final_query
    


if __name__ == '__main__':
    query = """
SELECT COUNT(*) 
FROM 
movie_companies mc,title t,movie_keyword mk 
WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mk.keyword_id=117;
"""
    greedy_selective_pairwise_join(query)
