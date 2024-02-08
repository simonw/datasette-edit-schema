import sqlite_utils
import json


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
            # Search for a value in this column that does not exist in the other table,
            # terminate early as soon as we find one since that shows this is not a
            # good foreign key candidate.
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


def potential_primary_keys(conn, table_name, columns, max_string_len=128):
    # First we run a query to check the max length of each column + if it has any nulls
    if not columns:
        return []
    selects = []
    for column in columns:
        selects.append('max(length("{}")) as "maxlen.{}"'.format(column, column))
        selects.append(
            'sum(case when "{}" is null then 1 else 0 end) as "nulls.{}"'.format(
                column, column
            )
        )
    sql = 'select {} from "{}"'.format(", ".join(selects), table_name)
    cursor = conn.cursor()
    cursor.execute(sql)
    row = cursor.fetchone()
    potential_columns = []
    for i, column in enumerate(columns):
        maxlen = row[i * 2] or 0
        nulls = row[i * 2 + 1] or 0
        if maxlen < max_string_len and nulls == 0:
            potential_columns.append(column)
    if not potential_columns:
        return []
    # Count distinct values in each of our candidate columns
    selects = ["count(*) as _count"]
    for column in potential_columns:
        selects.append('count(distinct "{}") as "distinct.{}"'.format(column, column))
    sql = 'select {} from "{}"'.format(", ".join(selects), table_name)
    cursor.execute(sql)
    row = cursor.fetchone()
    count = row[0]
    potential_pks = []
    for i, column in enumerate(potential_columns):
        distinct = row[i + 1]
        if distinct == count:
            potential_pks.append(column)
    return potential_pks


def examples_for_columns(conn, table_name):
    columns = sqlite_utils.Database(conn)[table_name].columns_dict.keys()
    ctes = [f'rows as (select * from "{table_name}" limit 1000)']
    unions = []
    params = []
    for i, column in enumerate(columns):
        ctes.append(
            f'col{i} as (select distinct "{column}" from rows '
            f'where ("{column}" is not null and "{column}" != "") limit 5)'
        )
        unions.append(f'select ? as label, "{column}" as value from col{i}')
        params.append(column)
    ctes.append("strings as ({})".format("\nunion all\n".join(unions)))
    ctes.append(
        """
    truncated_strings as (
    select 
        label,
        case 
        when length(value) > 30 then substr(value, 1, 30) || '...'
        else value
        end as value
    from strings
    where typeof(value) != 'blob'
    )
    """
    )
    sql = (
        "with {ctes} ".format(ctes=",\n".join(ctes))
        + "select label, json_group_array(value) as examples "
        "from truncated_strings group by label"
    )
    output = {}
    for column, examples in conn.execute(sql, params).fetchall():
        output[column] = list(map(str, json.loads(examples)))
    return output
