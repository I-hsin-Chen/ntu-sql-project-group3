import sqlparse
# 取得FROM(包括)之前的部分
def Fetch_base_stmt(stmt):
    parsed = sqlparse.parse(stmt)
    for stmt in parsed:
        for token in stmt.tokens:
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "FROM":
                return str(stmt)[: str(stmt).upper().rfind("FROM")].strip() + " FROM "
    return None


def Fetch_Projection(stmt):
    stmt_lower = stmt.lower()
    start = stmt_lower.find("select") + len("select")
    end = stmt_lower.find("from")
    if start == -1 or end == -1:
        return None
    substring = stmt[start:end].strip()
    substring = substring.split(',')
    
    return substring


# 獲得所有identifiers
def Fetch_identifiers(stmt):
    parsed = sqlparse.parse(stmt)
    tokens = parsed[0].tokens
    identifiers = []
    # for token in tokens:
    #     if isinstance(token, sqlparse.sql.Identifier):
    #         identifiers.append(token)
    identifiers = [token for token in tokens if isinstance(token, (sqlparse.sql.Identifier, sqlparse.sql.IdentifierList))]
    identifiers_values = []
    for identifier in identifiers:
        print(identifier)
        if isinstance(identifier, sqlparse.sql.IdentifierList):
            for iden in identifier:
                if(isinstance(iden, sqlparse.sql.Identifier)):
                    identifiers_values.append(iden)
        elif isinstance(identifier, sqlparse.sql.Identifier):
            identifiers_values.append(identifier)
    aliases = []
    real_name = []

    for identifier in identifiers_values:
        identifier = str(identifier)
        if " " in identifier:
            aliases.append(identifier[identifier.rfind(' ')+1:].strip())
            real_name.append(identifier[:identifier.rfind(' ')].strip())
        else:
            aliases.append(identifier)
            real_name.append(identifier)
    # exit()
    return real_name, aliases


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

if __name__ == '__main__':

    
    query = "SELECT COUNT(*) FROM cast_info ci CROSS JOIN (SELECT t.id t_id FROM title t CROSS JOIN movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year<1959) t_mi_idx WHERE t_mi_idx.t_id=ci.movie_id;"
    name, alias = Fetch_identifiers(query)
    print(name)
    print(alias)