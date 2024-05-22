import re
import itertools
from sqlalchemy import create_engine, text

def extract_tables_and_conditions(query):
    '''
    Extract tables and conditions as a list. 
    The first index in a table tuple is the table name and the second index is the alias.
    
    For example, if the query is :
    SELECT COUNT(*) 
    FROM movie_companies mc, title t, movie_info_idx mi_idx 
    WHERE t.id = mc.movie_id AND t.id = mi_idx.movie_id AND mi_idx.info_type_id = 112 AND mc.company_type_id = 2;
    
    Then the tables and conditions will be:
    tables = [('movie_companies', 'mc'), ('title', 't'), ('movie_info_idx', 'mi_idx')]
    conditions = ['t.id = mc.movie_id', 't.id = mi_idx.movie_id', 'mi_idx.info_type_id = 112', 'mc.company_type_id = 2']
    '''
    # extract table names
    tables = re.findall(r'FROM\s+(\w+)\s+(\w+)', query, re.IGNORECASE)
    tables += re.findall(r',\s*(\w+)\s+(\w+)', query, re.IGNORECASE)
    
    # extract conditions
    where_clause = re.search(r'WHERE(.*?);', query, re.DOTALL | re.IGNORECASE)
    conditions = where_clause.group(1).replace('\n', ' ').strip() if where_clause else ''
    conditions = [condition.strip() for condition in conditions.split('AND')]
    
    return tables, conditions

def extract_related_conditions(conditions, table1, table2, other_tables):
    '''
    This function extracts conditions that satisfy one of the following conditions:
    (1) Related to table both table1 and table2.
    (2) Related to table1 and not related to all the other tables.
    (3) Related to table2 and not related to all the other tables.
    '''
    related_conditions = []
    
    for condition in conditions:
        match_t1 = re.search(r'{}\.'.format(table1[1]), condition, re.IGNORECASE)
        match_t2 = re.search(r'{}\.'.format(table2[1]), condition, re.IGNORECASE)
        
        # Match the conditions that relate to both table1 and table2
        if match_t1 and match_t2:
            related_conditions.append(condition)
            
        # Match the conditions that relate to (table1 or table2) and not related to all the other tables
        elif (match_t1 or match_t2):
            for table in other_tables:
                if re.search(r'{}\.'.format(table[1]), condition, re.IGNORECASE):
                    break
                if table == other_tables[-1]:
                    related_conditions.append(condition)

    return related_conditions


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
            print(f'Conditions related to {table1} and {table2}: {node_pairs[(table1, table2)]}')
        
    
        # Estimating cardinality for each table pair (node pair)
        for node_pair in node_pairs:
            table1, table2 = node_pair
            condition = ' AND '.join(node_pairs[node_pair])
            query = f'SELECT COUNT(*) FROM {table1[0]} {table1[1]}, {table2[0]} {table2[1]} WHERE {condition};'
            
            import random
            # for testing purposes, generate random cardinalities temporarily
            cardinalities[(table1, table2)] = random.randint(0, 100)
        
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
            print(final_query)
            break
    
    


if __name__ == '__main__':
    query = """
SELECT COUNT(*) 
FROM 
movie_companies mc,title t,movie_keyword mk 
WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mk.keyword_id=117;
"""
    greedy_selective_pairwise_join(query)
