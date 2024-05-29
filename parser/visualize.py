# import sqlparse
from my_parser import *
from sqlparse.sql import Identifier, IdentifierList, Function
from sqlparse.tokens import Keyword
import re
from graphviz import Digraph
class TreeNode:
    def __init__(self, value):
        self.value = value
        self.children = []
        self.current_table = []

    def add_child(self, child_node):
        self.children.append(child_node)

def parse_sql(stmt):
    print("stmt = ", stmt)
    if stmt.strip()[0] == '(' and stmt.strip()[-1] == ')':
        stmt = stmt.strip()[1:-1]
    parsed = sqlparse.parse(stmt)[0]
    tables, aliases = Fetch_identifiers(stmt)
    conditions = Fetch_comparison(stmt)
    projections = Fetch_Projection(stmt)
    offset = len(projections)
    for projection in projections:
        if '*' in projection:
            offset = offset - 1
    tables = tables[offset:]
    aliases = aliases[offset:]

    aliases = dict(zip(tables, aliases))
    #print 所有資訊
    print("=====================================================================")
    print("Tables:", tables)
    print("Aliases:", aliases)
    print("Conditions:", conditions)
    print("Projections:", projections)
    print("=====================================================================")
    return aliases, conditions, projections

pattern = r"(=|>|<|' ')"

def build_tree(aliases, conditions, projections):
    last_node = None
    tables = list(aliases.values())
    # print(tables)
    # print(conditions)
    # print(included_table)
    while tables:
        new_node = TreeNode(tables[0])
        new_node.current_table.append(tables[0])
        tables.pop(0)
        
        
        select_conditions = []
        for condition in conditions:
            if new_node.value+'.' in condition:
                if re.search(pattern, condition):
                    right = re.split(pattern, condition)[-1].strip()
                if "'" in right or right.isdigit():
                    select_conditions.append(condition)
                    conditions.remove(condition)
        if select_conditions:
            root = TreeNode(f"σ {'AND '.join(select_conditions)}")
            root.current_table = new_node.current_table
            root.add_child(new_node)
            new_node = root
            
        if last_node is None: # 第一次進入
            last_node = new_node
            continue
        
        join_conditions = []
        
        for condition in conditions:
            for table in last_node.current_table:    
                if table+'.' in condition and new_node.current_table[0]+'.' in condition:
                    join_conditions.append(condition)
                    conditions.remove(condition)
        
        if join_conditions:
            root = TreeNode(f"⨝ {'AND '.join(join_conditions)}")
            root.current_table = last_node.current_table+new_node.current_table
            root.add_child(last_node)
            root.add_child(new_node)
        else:
            root = TreeNode("⨝")
            root.current_table = last_node.current_table+new_node.current_table
            root.add_child(last_node)
            root.add_child(new_node)
            
        last_node = root


    root = TreeNode(f"π {', '.join(projections)}")
    root.add_child(last_node)
    # for alias in aliases.values():
    #     if alias in root.value and alias not in last_node.value:
    #         new_node = TreeNode(alias)
    #         root.add_child(new_node)
        
    return root

def print_tree(node, level=0, prefix=""):
    if node is not None:
        print(" " * (level * 4) + prefix + str(node.value))
        for child in node.children:
            print_tree(child, level + 1, "└── ")
            
def traverse_tree(node, aliases):
    
    if node is not None:
        new_node = node  
        for key, alias in aliases.items():
            if "CROSS" not in key and "SELECT" not in key:
                continue
            if new_node.value == alias:
                print("KEY = ", key)
                aliases[key] = None
                a, c, p = parse_sql(key)
                new_node.add_child(build_tree(a, c, p))  
                traverse_tree(new_node, a)
        if 'σ' in new_node.value:
            for child in new_node.children:
                if 'σ' in child.value:
                    new_node.children = child.children
                    new_node.value = new_node.value + " AND" + child.value[1:]
                    new_node = traverse_tree(new_node, aliases)

        for i in range(len(new_node.children)):
            new_node.children[i] = traverse_tree(new_node.children[i], aliases)
        
        return new_node

def visualize_tree(node):
    dot = Digraph()
    dot.attr('node', fontname="Segoe UI Symbol bold", fontsize='20')  # 指定使用 Segoe UI Symbol 字型

    def add_nodes_edges(node, dot):
        label = node.value
        if node.children:
            dot.node(str(id(node)), label=label, shape='plaintext',style='bold')
        else:
            dot.node(str(id(node)), label=label, style='bold')
        for child in node.children:
            dot.edge( str(id(node)), str(id(child)), dir='none')
            add_nodes_edges(child, dot) 

    add_nodes_edges(node, dot)
    return dot

if __name__ == '__main__':
    # stmt = "SELECT COUNT(*) FROM title t, (SELECT mk_mi.mk_movie_id mk_mi_mk_movie_id,mc.movie_id mc_movie_id,mk_mi.mi_movie_id mk_mi_mi_movie_id FROM movie_companies mc, (SELECT mk.movie_id mk_movie_id,mi.movie_id mi_movie_id FROM movie_keyword mk CROSS JOIN movie_info mi WHERE mk.keyword_id=398) mk_mi WHERE mc.company_type_id=2) mc_mk_mi WHERE t.id=mc_mk_mi.mk_mi_mk_movie_id AND t.id=mc_mk_mi.mc_movie_id AND t.id=mc_mk_mi.mk_mi_mi_movie_id AND t.production_year>1950 AND t.production_year<2000;"
    # stmt = "SELECT COUNT(*) FROM movie_info_idx mi_idx CROSS JOIN title t CROSS JOIN movie_companies mc CROSS JOIN movie_keyword mk CROSS JOIN movie_info mi WHERE t.id=mi.movie_id AND t.id=mc.movie_id AND t.id=mk.movie_id AND t.id=mi_idx.movie_id AND mi_idx.info_type_id>101 AND mc.company_id>12501 AND t.kind_id<2 AND t.production_year<1992;"
    # 換成要測試的query
    stmt = "SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id=15551;"
    aliases, conditions, projections = parse_sql(stmt)

    root = build_tree(aliases, conditions, projections)
    traverse_tree(root, aliases)
    
    print("=====================================================================")
    print("Original query:\n", stmt)
    print("=====================================================================")
    print_tree(root)
    dot = visualize_tree(root)
    dot.render('tree', format='png', view=True)
    
