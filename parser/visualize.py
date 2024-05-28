import sqlparse
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
    if stmt.strip()[0] == '(' and stmt.strip()[-1] == ')':
        stmt = stmt.strip()[1:-1]
    parsed = sqlparse.parse(stmt)[0]
    tables, aliases = Fetch_identifiers(stmt)
    conditions = Fetch_comparison(stmt)
    projections = Fetch_Projection(stmt)
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
    for condition in conditions:
        select_conditions = ""
        join_conditions = ""
        root = None

        if re.search(pattern, condition):
            right = re.split(pattern, condition)[-1].strip()
        if "'" in right or right.isdigit():
            select_conditions = condition
        else:
            join_conditions = condition

        if select_conditions:
            root = TreeNode(f"σ {select_conditions}")
            if last_node is not None:
                root.current_table = last_node.current_table
            for key, alias in aliases.items():
                if alias+'.' in condition:
                    if alias not in root.current_table:
                        root.current_table.append(alias)
                        new_node = TreeNode(alias)
                        new_node.current_table.append(alias)
                        root.add_child(new_node)

        if join_conditions:
            root = TreeNode(f"⨝ {join_conditions}")
            if last_node is not None:
                root.current_table = last_node.current_table
            for key, alias in aliases.items():
                if alias+'.' in condition:
                    if alias not in root.current_table:
                        root.current_table.append(alias)
                        new_node = TreeNode(alias)
                        new_node.current_table.append(alias)
                        root.add_child(new_node)

        if last_node is not None:
            root.add_child(last_node)
        last_node = root

    root = TreeNode(f"π {', '.join(projections)}")
    root.add_child(last_node)
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
                a, c, p = parse_sql(key)
                new_node.add_child(build_tree(a, c, p))  
                traverse_tree(new_node, a)
        if 'σ' in new_node.value:
            for child in new_node.children:
                if 'σ' in child.value:
                    new_node.children = child.children
                    new_node.value = new_node.value + " AND" + child.value[1:]

        for i in range(len(new_node.children)):
            new_node.children[i] = traverse_tree(new_node.children[i], aliases)
        
        return new_node

def visualize_tree(node):
    dot = Digraph()
    dot.attr('node', fontname="Segoe UI Symbol")  # 指定使用 Segoe UI Symbol 字型

    def add_nodes_edges(node, dot):
        label = node.value
        dot.node(str(id(node)), label=label)
        for child in node.children:
            dot.edge(str(id(node)), str(id(child)))
            add_nodes_edges(child, dot) 

    add_nodes_edges(node, dot)
    return dot

if __name__ == '__main__':
    # 換成要測試的query
    stmt = "SELECT COUNT(*) FROM (SELECT mk.movie_id mk_movie_id,mi.movie_id mi_movie_id FROM movie_keyword mk CROSS JOIN movie_info mi WHERE mk.keyword_id>1633) mi_mk CROSS JOIN (SELECT mi_idx_t.t_id mi_idx_t_t_id FROM (SELECT t.id t_id FROM title t CROSS JOIN movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id>1) mi_idx_t CROSS JOIN movie_companies mc WHERE mi_idx_t.t_id=mc.movie_id) mc_mi_idx_t WHERE mc_mi_idx_t.mi_idx_t_t_id=mi_mk.mk_movie_id AND mc_mi_idx_t.mi_idx_t_t_id=mi_mk.mi_movie_id;"
    aliases, conditions, projections = parse_sql(stmt)

    root = build_tree(aliases, conditions, projections)
    traverse_tree(root, aliases)
    
    print("=====================================================================")
    print("Original query:\n", stmt)
    print("=====================================================================")
    print_tree(root)
    dot = visualize_tree(root)
    dot.render('tree', format='png', view=True)
    
