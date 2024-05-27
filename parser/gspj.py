import re
import itertools
from sqlalchemy import create_engine, text
from utils import *

def remove_duplicates(lst):
    seen = set()
    return [x for x in lst if not (x in seen or seen.add(x))]

def greedy_selective_pairwise_join(query):
    '''
    This function performs a greedy selective pairwise join (GSPJ) on the given query.
    The returned query is the final query after performing GSPJ.
    '''
    
    # Extract all the tables and conditions from the query
    original_query = query
    tables, conditions = extract_tables_and_conditions(query)
    
    while True:
        if(len(tables) == 1):
            return tables[0][0].strip("()") + ";"
        
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
            cardinalities[(table1, table2)] = cardinality_estimation(query) if query.upper().count("SELECT") == 1 \
                else cardinality_estimation(flatten(query, original_query))
        
        best_combination = min(cardinalities, key=cardinalities.get)
        # print(f'Best join is between {best_combination[0]} and {best_combination[1]} with cardinality {cardinalities[best_combination]}')
        
        table1, table2 = best_combination
        if node_pairs[best_combination] == []:
            raise Exception(f'Parsing error : No conditions found for joining {table1} and {table2}')
        match_condition = ' AND '.join(node_pairs[best_combination])
        combined_alias = f"{table1[1]}_{table2[1]}"
        
        # Remove the conditions that are already used in the best combination
        conditions = [c for c in conditions if c not in node_pairs[best_combination]]
        
        # Update the all the aliases in the remaining conditions
        selected_columns = []
        for i, condition in enumerate(conditions):
            for t in (table1, table2):
                match = re.search(r'{}\.(\w+)'.format(t[1]), condition, re.IGNORECASE)
                if match:
                    # column_alias : Alias of a column in the subquery, Ex : t.id --> t_id
                    column_alias = "_".join(match.group().split('.'))
                    
                    # selected_columns : columns be selected in the subquery, Ex : SELECT t.id t_id FROM ...
                    selected_columns.append(f'{match.group()} {column_alias}')
                    
                    # if the column will be used by the outer query, then replace the column name with the alias
                    conditions[i] = conditions[i].replace(match.group(), f'{combined_alias}.{column_alias}') \
                        if match.group() in conditions[i] \
                        else re.sub(r'{}\.(\w+)'.format(t[1]), f'{combined_alias}.\\1', conditions[i])
                    
        selected_columns = ','.join(remove_duplicates(selected_columns))
        subquery = f'(SELECT {selected_columns} FROM {table1[0]} {table1[1]},{table2[0]} {table2[1]}\nWHERE {match_condition})' \
            if selected_columns \
            else f'(SELECT * FROM {table1[0]} {table1[1]},{table2[0]} {table2[1]} WHERE {match_condition})'
        
        new_table = (subquery, combined_alias)
        tables.remove(table1)
        tables.remove(table2)
        tables.append(new_table)
        
        # If there are only two tables left, then simply join them and return the final query
        if len(tables) == 2:
            remaining_conditions = ' AND '.join(conditions)
            final_query = f'SELECT COUNT(*) \nFROM {tables[0][0]} {tables[0][1]},{tables[1][0]} {tables[1][1]} \nWHERE \n{remaining_conditions};' \
                if remaining_conditions else f'SELECT COUNT(*) FROM {tables[1][0]} {tables[1][1]},{tables[0][0]} {tables[0][1]};'
            
            final_query = to_cross_join(final_query)
            # print("The final result is :\n" + final_query)
            return final_query


if __name__ == '__main__':
    query = """
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.id=mi_idx.movie_id AND t.production_year>2010 AND t.kind_id=1 AND mi.info_type_id=8 AND mi_idx.info_type_id=101;
"""
    print("The original query is :" + to_cross_join(query))
    gspj_query = greedy_selective_pairwise_join(query)
    print("The GSPJ query is :\n" + gspj_query)
