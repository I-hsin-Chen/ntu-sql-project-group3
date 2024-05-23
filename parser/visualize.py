import sqlparse
from parse import Fetch_comparison
from sqlparse.sql import Identifier, IdentifierList, Comparison, Where, Function
from sqlparse.tokens import Keyword


class TreeNode:
    def __init__(self, value):
        self.value = value
        self.children = []
        self.current_table = []

    def add_child(self, child_node):
        self.children.append(child_node)


def parse_sql(stmt):
    parsed = sqlparse.parse(stmt)[0]
    tables = []
    conditions = Fetch_comparison(stmt)
    projections = []
    aliases = {}
    from_seen = False
    select_seen = False

    for token in parsed.tokens:
        if token.ttype is Keyword.DML and "SELECT" in token.value.upper():
            select_seen = True
        elif token.ttype is Keyword and token.value.upper() == "FROM":
            from_seen = True
            select_seen = False
        elif token.ttype is Keyword and "WHERE" in token.value.upper():
            from_seen = False
        elif from_seen and isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                tables.append(identifier.get_real_name())
                if identifier.has_alias():
                    aliases[identifier.get_real_name()] = identifier.get_alias()
        elif from_seen and isinstance(token, Identifier):
            tables.append(token.get_real_name())
        elif select_seen and isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                projections.append(str(identifier))
        elif select_seen and isinstance(token, Function):
            projections.append(str(token))
        elif select_seen and isinstance(token, Identifier):
            print("identifier:", identifier)
            projections.append(str(token))
    if projections == []:
        start = stmt.lower().find("select") + len("select")
        end = stmt.lower().find("from")
        substring = stmt[start:end].strip()
        projections.append(substring)
    print("Tables:", tables)
    print("Aliases:", aliases)
    print("Conditions:", conditions)
    print("Projections:", projections)
    return tables, aliases, conditions, projections


import re

pattern = r"(=|>|<|' ')"


def build_tree(tables, aliases, conditions, projections):
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
            # print("select = ", select_conditions)
            root = TreeNode(f"σ {select_conditions}")

            if last_node is not None:
                root.current_table = last_node.current_table
            for key, alias in aliases.items():
                if alias in condition:
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
                if alias in condition:
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


# # stmt = "SELECT P.Pnumber, P.Dnum, E.Lname, E.Address, E.Bdate FROM PROJECT P, DEPARTMENT D, EMPLOYEE E WHERE P.Dnum=D.Dnumber AND D.Mgr_ssn=E.Ssn AND P.Plocation= 'Stafford';"
stmt = "SELECT COUNT(*) FROM movie_companies mc,title t,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mi_idx.info_type_id=112 AND mc.company_type_id=2;"
# Parse the SQL statement
tables, aliases, conditions, projections = parse_sql(stmt)

# Build the query tree
root = build_tree(tables, aliases, conditions, projections)
print("=====================================================================")
print("Original query:\n", stmt)
print("=====================================================================")
# Visualize the query tree
print_tree(root)
