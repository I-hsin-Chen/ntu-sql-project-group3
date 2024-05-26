import re
import itertools
from sqlalchemy import create_engine, text
from utils import *
import csv


def Brutal_Force(query):
    '''
    This function performs a greedy selective pairwise join (GSPJ) on the given query.
    The returned query is the final query after performing GSPJ.
    '''
    query_list = []
    # Extract all the tables and conditions from the query
    tables, conditions = extract_tables_and_conditions(query)

    def brutal_force(tables, conditions):

        if len(tables) == 2:
            remaining_conditions = ' AND '.join(conditions)
            final_query = f'SELECT COUNT(*) \nFROM {tables[0][0]} {tables[0][1]}, {tables[1][0]} {tables[1][1]} \nWHERE \n{remaining_conditions};' \
                if remaining_conditions else f'SELECT COUNT(*) FROM {tables[0][0]} {tables[0][1]}, {tables[1][0]} {tables[1][1]};'
            final_query = to_cross_join(final_query)
            final_query = final_query.replace('\n', ' ').replace('  ', ' ')

            query_list.append(final_query)
            return final_query

        node_pairs = {}
        buf_conditions = conditions

        # Loop over all pairs of tables and extract conditions related to them
        for table1, table2 in list(itertools.combinations(tables, 2)):
            other_tables = [
                table for table in tables if table not in (table1, table2)]
            node_pairs[(table1, table2)] = extract_related_conditions(
                conditions, table1, table2, other_tables)
            if node_pairs[(table1, table2)] == []:
                raise Exception(f'Parsing error : No conditions found for joining {table1} and {table2}')

            match_condition = ' AND '.join(node_pairs[(table1, table2)])
            combined_alias = f"{table1[1]}_{table2[1]}"

            # Remove the conditions that are already used in the best combination
            conditions = [
                c for c in conditions if c not in node_pairs[(table1, table2)]]

            # Update the all the aliases in the remaining conditions
            selected_columns = []
            for i, condition in enumerate(conditions):
                for t in (table1, table2):
                    match = re.search(
                        r'{}\.(\w+)'.format(t[1]), condition, re.IGNORECASE)
                    if match:
                        # column_alias : Alias of a column in the subquery, Ex : t.id --> t_id
                        column_alias = "_".join(match.group().split('.'))

                        # selected_columns : columns be selected in the subquery, Ex : SELECT t.id t_id FROM ...
                        selected_columns.append(
                            f'{match.group()} {column_alias}')

                        # if the column will be used by the outer query, then replace the column name with the alias
                        conditions[i] = conditions[i].replace(match.group(), f'{combined_alias}.{column_alias}') \
                            if match.group() in conditions[i] \
                            else re.sub(r'{}\.(\w+)'.format(t[1]), f'{combined_alias}.\\1', conditions[i])

            selected_columns = ','.join(selected_columns)
            subquery = f'(SELECT {selected_columns} FROM {table1[0]} {table1[1]}, {table2[0]} {table2[1]} WHERE {match_condition})' \
                if selected_columns \
                else f'(SELECT * FROM {table1[0]} {table1[1]}, {table2[0]} {table2[1]} WHERE {match_condition})'

            new_table = (subquery, combined_alias)
            tables.remove(table1)
            tables.remove(table2)
            tables.append(new_table)
            # ---- recursive
            brutal_force(tables, conditions)
            # -----
            tables.remove(new_table)
            tables.append(table1)
            tables.append(table2)
            conditions = buf_conditions

    brutal_force(tables, conditions)

    return query_list


if __name__ == '__main__':
    query = """
    SELECT COUNT(*) FROM movie_keyword mk,title t,cast_info ci WHERE t.id=mk.movie_id AND t.id=ci.movie_id AND t.production_year>2014 AND mk.keyword_id=8200;
    """
    print("The original query is :" + to_cross_join(query))
    all_possible_query = Brutal_Force(query)

    print("possible query : ")
    with open('./bf_output/output.txt', 'w') as file:
        for query in all_possible_query:
            file.write(query + '\n')
            print(query)
    
    
