import sqlite_utils


def get_primary_keys(conn):
    db = sqlite_utils.Database(conn)
    primary_keys = []
    for table in db.tables:
        if "_fts_" in table.name:
            continue
        pks = table.pks
        if pks == ["rowid"]:
            continue
        if len(pks) != 1:
            continue
        pk = pks[0]
        # Is that a str or int?
        pk_type = table.columns_dict[pk]
        if pk_type in (str, int):
            primary_keys.append((table.name, pk, pk_type))
    return primary_keys


def potential_foreign_keys(conn, table_name, columns, other_table_pks):
    potentials = {}
    cursor = conn.cursor()
    for column in columns:
        potentials[column] = []
        for other_table, other_column, _ in other_table_pks:
            query = """
                select "{table}"."{column}"
                from "{table}"
                where not exists (
                    select 1
                    from "{other_table}"
                    where "{table}"."{column}" = "{other_table}"."{other_column}"
                )
                limit 1;
            """.format(
                table=table_name,
                column=column,
                other_table=other_table,
                other_column=other_column,
            )
            cursor.execute(query)
            if cursor.fetchone() is None:
                potentials[column].append((other_table, other_column))
    return potentials
