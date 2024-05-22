import sqlparse

# stmt = "SELECT * FROM Graph AS g1, Graph AS g2, Graph AS g3 where g2.dst = g3.src AND g1.dst = g2.src AND g3.dst = g1.src;"
stmt = input("Please input your SQL query: ")

parsed = sqlparse.parse(stmt)
tokens = parsed[0].tokens
for token in tokens:
    print(type(token), token.ttype, token.value)


# 取得FROM(包括)之前的部分
def Fetch_base_stmt(stmt):
    parsed = sqlparse.parse(stmt)
    for stmt in parsed:
        for token in stmt.tokens:
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "FROM":
                return str(stmt)[: str(stmt).upper().rfind("FROM")].strip() + " FROM "
    return None


base = Fetch_base_stmt(stmt)
print(base)


# 獲得所有identifiers
def Fetch_identifiers(stmt):
    parsed = sqlparse.parse(stmt)
    tokens = parsed[0].tokens
    identifiers = [
        token
        for token in tokens
        if isinstance(token, (sqlparse.sql.Identifier, sqlparse.sql.IdentifierList))
    ]
    identifiers_values = []
    for identifier in identifiers:
        for token in identifier.tokens:
            if isinstance(token, sqlparse.sql.Identifier):
                identifiers_values.append(token.value)
            elif isinstance(token, sqlparse.sql.IdentifierList):
                for iden in token.tokens:
                    identifiers_values.append(iden.value)
    return identifiers_values


id_list = Fetch_identifiers(stmt)
# print(id_list)


# 獲得所有conditions
def Fetch_comparison(stmt):
    parsed = sqlparse.parse(stmt)
    conditions = [
        token for token in parsed[0].tokens if isinstance(token, sqlparse.sql.Where)
    ]
    comparisons = []
    for condition in conditions:
        comparisons.extend(
            [
                str(token)
                for token in condition.tokens
                if isinstance(token, sqlparse.sql.Comparison)
            ]
        )

    return comparisons


cmp = Fetch_comparison(stmt)
# print(cmp)


# 將query拆成多個queries
def split_query(stmt):
    base = Fetch_base_stmt(stmt)
    id_list = Fetch_identifiers(stmt)
    cmp = Fetch_comparison(stmt)

    id = []
    for i in id_list:
        if " " in i:
            id.append(i.split(" ")[-1])
    queries = []
    for c in cmp:
        query = base
        for idx, i in enumerate(id):
            if i in c:
                query += id_list[idx] + ", "
        if query.endswith(", "):
            query = query[:-2]
        query += " WHERE " + c + ";"
        queries.append(query)

    return queries


# 取得WHERE(包括)之前的部分
def get_part_before_where(stmt):
    parsed = sqlparse.parse(stmt)
    parsed_statement = parsed[0]
    pre_where_clause = ""
    for token in parsed_statement.tokens:
        if "WHERE" in token.value.upper():
            break
        pre_where_clause += str(token)

    return pre_where_clause.strip()


# 重新排序 WHERE 條件
def comparison_reorder(stmt, new_order):
    conditions = []
    parsed = sqlparse.parse(stmt)
    for token in parsed[0].tokens:
        if isinstance(token, sqlparse.sql.Where):
            for subtoken in token.tokens:
                if isinstance(subtoken, sqlparse.sql.Comparison):
                    conditions.append(str(subtoken).strip())
    conditions = [conditions[i] for i in new_order]
    before_where = get_part_before_where(stmt)
    reordered_sql_query = before_where + " WHERE " + " AND ".join(conditions) + ";"
    return reordered_sql_query


print("=====================================================================")
print("Original query:\n", stmt, "\nSplit queries:")
for q in split_query(stmt):
    print(q)
print("=====================================================================")
print("Original query:\n", stmt)
print("Reordered query:\n", comparison_reorder(stmt, [2, 1, 0]))
print("=====================================================================")
