import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cardinality import get_cardinality
import re

def cardinality_estimation(query):
    '''
    This function returns the cardinality of the given query using function call to DeepDB.
    You suppose to have a imdb-benchmark folder in the parent directory of this project.
    In the imdb-benchmark folder, 
    you should have a folder named spn_ensembles containing the two .pkl files.
    '''
    ensemble_location = '../imdb-benchmark/spn_ensembles/ensemble_relationships_imdb-light_1000000.pkl'
    pairwise_rdc_path = '../imdb-benchmark/spn_ensembles/pairwise_rdc.pkl'
    rdc_spn_selection = True
    
    cardinality = get_cardinality(ensemble_location, query, None, rdc_spn_selection, pairwise_rdc_path)
    return cardinality

def to_cross_join(sql_query):
    '''
    Convert the given query to a cross join query.
    '''
    
    # Regular expression to match the FROM clause and capture its content
    from_clause_pattern = r'FROM\s+([^;]+)WHERE'
    matches = re.search(from_clause_pattern, sql_query, re.IGNORECASE)
    
    if matches:
        from_clause = matches.group(1)
        # Split the FROM clause by commas not enclosed in parentheses
        tables = re.split(r',(?![^()]*\))', from_clause)
        # Recursively convert each subquery to a cross join query
        tables = [to_cross_join(table) for table in tables]
        cross_join_clause = ' CROSS JOIN '.join(tables)
        
        # Replace the old FROM clause with the new one
        new_sql_query = re.sub(from_clause_pattern, f'FROM {cross_join_clause}WHERE', sql_query, flags=re.IGNORECASE)
        return new_sql_query
    else:
        return sql_query


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
    (1) Related to both table1 and table2.
    (2) Related to table1 and not related to all the other tables.
    (3) Related to table2 and not related to all the other tables.
    
    Parameters:
    (1) conditions : list of all the remaining conditions
    (2) table1 / table2 : tuple of table name and alias
    (3) other_tables : all the other tables except table1 and table2
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