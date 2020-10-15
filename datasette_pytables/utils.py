from moz_sql_parser import parse
import re


def parse_sql(sql, params):
    # Table name
    sql = re.sub(r'(?i)from \[(.*)]', r'from "\g<1>"', sql)
    # Params
    for param in params:
        sql = sql.replace(":" + param, param)

    try:
        parsed = parse(sql)
    except:
        # Propably it's a PyTables expression
        for token in ['group by', 'order by', 'limit', '']:
            res = re.search('(?i)where (.*)' + token, sql)
            if res:
                modified_sql = re.sub('(?i)where (.*)(' + token + ')', r'\g<2>', sql)
                parsed = parse(modified_sql)
                parsed['where'] = res.group(1).strip()
                break

    # Always a list of fields
    if type(parsed['select']) is not list:
        parsed['select'] = [parsed['select']]

    return parsed
