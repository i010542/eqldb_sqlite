import sqlite3


def normalize(v):
    return v.replace('"', '""')


def rowset2table(con, rowset, table_name, sp, callback, istart, itotal):
    if len(rowset) == 0:
        return '', []
    cursor = con.cursor()

    field_names = []
    sql = 'CREATE TABLE ' + table_name + '('
    first = True
    for col in rowset[0]:
        field_name = col[0][1:]
        field_names.append(field_name)
        if not first:
            sql += ','
        first = False
        sql += field_name + ' varchar(200)'
    sql += ');'
    cursor.execute(sql)
    con.commit()

    sp_index = 0
    i = 0
    for row in rowset:
        sql = 'INSERT INTO ' + table_name + ' values ('
        first = True
        for col in row:
            if not first:
                sql += ','
            first = False
            try:
                v = col[2]
                if type(v) == tuple:
                    v = table_name + '__sp__' + str(sp_index)
                    sp[v] = col[2]
                    sp_index += 1
                sql += '"' + normalize(v) + '"'
            except:
                pass
        sql += ');'
        #print(sql, col)
        cursor.execute(sql)
        con.commit()
        i += 1
        callback(istart+i, itotal, '')
    return field_names


def create_index(con, table_name, field_list):
    sql = 'CREATE INDEX ' + table_name + '_index_' + '_'.join(field_list) + ' on ' + table_name + '(' + ','.join(field_list) + ');'
    cursor = con.cursor()
    cursor.execute(sql)
    cursor.close()


def rowset_join(rowset1, rowset2, callback):
    con = sqlite3.connect(':memory:')
    sp = {}
    tab1 = 't' + str(id(rowset1))
    tab2 = 't' + str(id(rowset2))
    f1 = rowset2table(con, rowset1, tab1, sp, callback, 0, len(rowset1)+len(rowset2))
    f2 = rowset2table(con, rowset2, tab2, sp, callback, len(rowset1), len(rowset1)+len(rowset2))
    fcomm = [val for val in f1 if val in f2]
    if len(fcomm) > 0:
        create_index(con, tab1, fcomm)
        create_index(con, tab2, fcomm)
    fall1 = list(map(lambda x: tab1+'.'+x + ' as ' + x, f1))
    fall2 = list(set(f2)-set(fcomm))
    fall = fall1 + fall2
    fallx = f1 + fall2
    sql = 'select ' + ','.join(fall) + ' from ' + tab1 + ',' + tab2
    if len(fcomm) > 0:
        cond = list(map(lambda x: tab1 + '.' + x + '=' + tab2 + '.' + x, fcomm))
        sql += ' where ' + ' and '.join(cond)
    cursor = con.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    result = []
    for row in rows:
        row2 = []
        for i in range(0, len(fallx)):
            v = row[i]
            if sp.get(v) is not None:
                v = sp.get(v)
            col = ('?'+fallx[i], '', v)
            row2.append(col)
        row2 = tuple(row2)
        result.append(row2)
        callback(i+1, len(fallx), row2)
    cursor.close()
    con.close()
    return result


def get_field_names(con, table_name):
    cursor = con.cursor()
    cursor.execute('pragma table_info({})'.format(table_name))
    field_names = cursor.fetchall()
    field_names = [x[1] for x in field_names]
    cursor.close()
    return field_names
