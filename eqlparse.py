# An implementation of EQL, zhaohj@sina.com, 2020-6-20
#

from ply import *
import eqllex

tokens = eqllex.tokens

precedence = (
)


# 程序 ::= 语句 | 程序 ',' 语句 | 数据库操纵语句
def p_program(p):
    """program : statement
            | program COMMA statement
            | db
    """
    if len(p) == 2 and p[1]:
        p[0] = (p[1],)
    if len(p) == 4:
        p[0] = p[1] + (p[3],)




# 语句 ::= 查询语句 | 计算语句 | 答案语句
def p_statement(p):
    """statement : statement_query
        | statement_compute
        | statement_answer

    """
    if len(p) == 2:
        p[0] = ('statement', p[1])


# 查询语句 ::= 查询表达式列表 | 查询表达式列表 [分组表达式] [排序表达式] {过滤表达式}
def p_statement_query(p):
    """statement_query : query_list filter_list group order limit
    """
    p[0] = ('query',)
    for i in range(1, len(p)):
        p[0] += (p[i],)


# 查询表达式列表 ::= 查询表达式 | 查询表达式列表 逻辑运算符 查询表达式
def p_query_list(p):
    """query_list : query
            | query_list logic query
    """
    if len(p) == 2:
        p[0] = (('\\and', p[1]),)
    if len(p) == 4:
        p[0] = p[1] + ((p[2], p[3]),)


# 查询表达式 ::= 事实表达式 | 比较表达式
def p_query(p):
    """query : fact
            | compare
    """
    if len(p) == 2:
        p[0] = p[1]


# 逻辑运算符 ::= '\and' | '\or' | '\not' | ('\and' '\not') | ('\or' '\not')
def p_logic(p):
    """logic : AND
            | OR
            | NOT
    """
    if len(p) == 2:
        p[0] = p[1]


# 事实表达式 ::= spo表达式 | spo表达式 qv表达式
def p_fact(p):
    """fact : spo
            | spo LPAREN qvlist RPAREN
            | repeat
            | sp
    """
    if len(p) == 2:
        p[0] = ('fact', p[1])
    if len(p) == 5:
        p[0] = ('fact', p[1], p[3])

def p_sp(p):
    """sp : STRING
        | sp DOT STRING
    """
    if len(p) == 2:
        p[0] = ('sp', p[1],)
    if len(p) == 4:
        p[0] = p[1] + (p[3],)

def p_repeat(p):
    """repeat : s COLON p LPAREN REPEAT COLON range RPAREN COLON o"""
    p[0] = ('repeat',p[1],p[3],p[10],p[7])

def p_range_number(p):
    """range : STRING"""
    p[0] = ('range', p[1], p[1])

def p_range_number_addmark(p):
    """range : STRING ADDMARK"""
    p[0] = ('range', p[1], '9')

def p_range_number_submark(p):
    """range : STRING SUBMARK STRING"""
    p[0] = ('range', p[1], p[3])

def p_range_number_tilde(p):
    """range : STRING TILDE STRING"""
    p[0] = ('range', p[1], p[3])

# spo表达式 ::= s表达式 ':' p表达式 ':' o表达式
def p_spo(p):
    """spo : s COLON p COLON o"""
    if len(p) == 6:
        p[0] = ('spo', p[1], p[3], p[5])


# s表达式 ::= 变量 | 常量 | '(' s表达式 ':' p表达式 ')'
def p_s_var(p):
    """s : VAR
    """
    if len(p) == 2:
        p[0] = ('var', p[1])


def p_s_string(p):
    """s : STRING
    """
    if len(p) == 2:
        p[0] = ('string', p[1])


def p_s_var_string(p):
    """s : LPAREN VAR COLON STRING RPAREN
    """
    if len(p) == 6:
        p[0] = ('var_string', p[2], p[4])


def p_s_string_string(p):
    """s : LPAREN STRING COLON STRING RPAREN
    """
    if len(p) == 6:
        p[0] = ('string_string', p[2], p[4])


# p表达式 ::= 变量 | 常量
def p_p_var(p):
    """p : VAR
    """
    if len(p) == 2:
        p[0] = ('var',p[1])


def p_p_string(p):
    """p : STRING
    """
    if len(p) == 2:
        p[0] = ('string',p[1])


# o表达式 ::= 变量 | 常量
def p_o_var(p):
    """o : VAR
    """
    if len(p) == 2:
        p[0] = ('var',p[1])


def p_o_string(p):
    """o : STRING
    """
    if len(p) == 2:
        p[0] = ('string',p[1])


def p_qvlist(p):
    """qvlist : qv
            | qvlist COMMA qv
    """
    if len(p) == 2:
        p[0] = ('qvlist', p[1])
    if len(p) == 4:
        p[0] = (p[1] + (p[3],))

# qv表达式 ::= '?' | '(' q表达式 ':' v表达式 ')'
def p_qv(p):
    """qv : VAR
        | q COLON v
    """
    if len(p) == 2:
        p[0] = ('var', p[1])
    if len(p) == 4:
        p[0] = ('qv', p[1], p[3])


# q表达式 ::= 变量 | 常量
def p_q_var(p):
    """q : VAR
    """
    if len(p) == 2:
        p[0] = ('var',p[1])


def p_q_string(p):
    """q : STRING
    """
    if len(p) == 2:
        p[0] = ('string',p[1])


# v表达式 ::= 变量 | 常量
def p_v_var(p):
    """v : VAR
    """
    if len(p) == 2:
        p[0] = ('var',p[1])


def p_v_string(p):
    """v : STRING
    """
    if len(p) == 2:
        p[0] = ('string',p[1])


# 分组表达式 ::= '\group by' 属性表达式
def p_group(p):
    """group : GROUPBY prop_expr
        | empty
    """
    if len(p) == 2:
        p[0] = None
    if len(p) == 3:
        p[0] = ('groupby', p[1], p[2])


# 排序表达式 ::= '\order by' 属性表达式 {'desc' | 'asc'}
def p_order(p):
    """order : ORDERBY prop_expr order_op
        | empty
    """
    if len(p) == 2:
        p[0] = None
    if len(p) == 4:
        p[0] = ('orderby', p[1], p[2], p[3])


# limit n | unlimit
def p_limit(p):
    """limit : LIMIT STRING
        | UNLIMIT
        | empty
    """
    if len(p) == 2:
        if p[1] is None:
            p[0] = None
        else:
            p[0] = ('limit','0')
    if len(p) == 3:
        p[0] = ('limit', p[2])

def p_order_op(p):
    """order_op : DESC
            | ASC
            | empty
    """
    if len(p) == 2:
        p[0] = p[1]


# 过滤表达式 ::= '\filter' 比较表达式
def p_filter_list(p):
    """filter_list : filter
            | filter_list filter
    """
    if len(p) == 2 and p[1]:
        p[0] = ('filter_list', p[1],)
    if len(p) == 3:
        p[0] = p[1] + (p[2],)


def p_filter(p):
    """filter : FILTER compare
        | empty
    """
    if len(p) == 2:
        p[0] = None
    if len(p) == 3:
        p[0] = ('filter', p[1], p[2])


# 比较表达式 ::= 属性表达式 比较运算符 常量 | '(' 函数表达式 比较运算符 常量 ')' | 属性表达式 匹配运算符 "'" 常量 "'"
def p_compare(p):
    """compare : prop_expr compare_op STRING
        | LPAREN function compare_op STRING RPAREN
        | prop_expr MATCH STRING
    """
    if len(p) == 4:
        p[0] = ('compare',p[1], p[2], p[3])
    if len(p) == 6:
        p[0] = ('compare',p[2], p[3], p[4])


# 函数名 ::= 'count' | 'avg' | 'sum' | 'max' | 'min'
def p_function_name(p):
    """function_name : COUNT
        | AVG
        | SUM
        | MAX
        | MIN
    """
    if len(p) == 2:
        p[0] = p[1]


# 函数表达式 ::= 函数名 '(' 属性表达式 ')'
def p_function(p):
    """function : function_name LPAREN prop_expr RPAREN
        | function_name LPAREN prop_expr OF VAR RPAREN
    """
    if len(p) == 5:
        p[0] = ('function', p[1], p[3], '')
    if len(p) == 7:
        p[0] = ('function', p[1], p[3], p[5])


# 属性表达式 ::= 变量 | 属性表达式 '.' 常量 //s.p1.p2.p3......
def p_prop_expr(p):
    """prop_expr : VAR
        | prop_expr DOT STRING
    """
    if len(p) == 2:
        p[0] = ('prop_expr', p[1],)
    if len(p) == 4:
        # p[0] = p[1] + (p[3],)
        p[0] = (p[1][0], p[1][1] + '.' + p[3])


# 比较运算符 ::= '=' | '!=' | '>' | '>=' | '<' | '<='
def p_compare_op(p):
    """compare_op : EQ
        | NOTEQ
        | GT
        | GE
        | LT
        | LE
    """
    if len(p) == 2:
        p[0] = p[1]


# 计算语句 ::= 变量 '=' 函数表达式
def p_statement_compute(p):
    """statement_compute : VAR EQ function
    """
    if len(p) == 4:
        p[0] = ('compute', p[1], p[3])


# 答案语句 ::= 'ANS' 属性表达式 | 答案语句 ',' 属性表达式
def p_statement_answer(p):
    """statement_answer : ANS prop_expr
        | statement_answer COMMA prop_expr
    """
    if len(p) == 3:
        p[0] = ('answer', p[2][1],)
    if len(p) == 4:
        # p[0] = p[1] + (p[3],)
        #print('answer: p[1]=', p[1],p[2],p[3])
        p[0] = (p[1][0], p[1][1] + ',' + p[3][1])

# 数据库操作::= 插入语句 | 修改语句 | 删除语句 | 插入语句 参考语句 | 修改语句 参考语句
def p_db(p):
    """db : add
        | add ref
        | change
        | change ref
        | delete
    """
    if len(p)==2 and p[1]:
        p[0]=('db',p[1])
    if len(p)==3:
        p[0]=('db',p[1],('REF',p[2]))

# 插入语句::= 'ADD' 事实表达式 | 'ADD' 事实表达式  参考语句
def p_add(p):
    """add : ADD data
    """
    if len(p)==3:
        p[0]=('add',p[2])


#修改语句::='CHANGE' 事实表达式 'CHANGETO' 事实表达式 | 'CHANGE' 事实表达式 'CHANGETO' 事实表达式  参考语句
def p_change(p):
    """change : CHANGE data CHANGETO data
    """
    if len(p)==5:
        p[0]=('change',(p[2],p[4]))


#删除语句::='DELETE' spo表达式
def p_delete(p):
    """delete : DELETE data_spo
    """
    if len(p)==3:
        p[0]=('delete',('data',(p[2],)))

#参考语句::=参考表达式 | 参考语句 ',' 参考表达式
def p_ref(p):
    """ref : refsta
           | ref COMMA refsta
    """
    if len(p)==2:
        p[0]=(p[1],)
    if len(p)==4:
        p[0]=p[1]+(p[3],)


#参考表达式::='REF' STRING
def p_refsta(p):
    """refsta : REF COLON STRING
    """
    if len(p)==4:
        p[0] = p[3]

def p_data(p):
    """data : data_spo
            | data_spo LPAREN data_qv RPAREN
    """
    if len(p)==2:
        p[0]=('data',(p[1],))
    if len(p)==5:
        p[0]=('data',(p[1],p[3]))
def p_data_spo(p):
    """data_spo : STRING COLON STRING COLON STRING
    """
    if len(p)==6:
        p[0]=("data_spo",p[1],p[3],p[5])


def p_data_qv(p):
    """data_qv :  STRING COLON STRING
            |  data_qv COMMA STRING COLON STRING
    """
    if len(p)==4:
        p[0]=("data_qv",(p[1],p[3]))
    if len(p)==6:
        p[0]=p[1]+((p[3],p[5]),)






def p_empty(p):
    """empty : """


def p_error(p):
    if not p:
        print("SYNTAX ERROR AT EOF")
    else:
        if eqlparser.error_p is None:
            eqlparser.error_p = p
            eqlparser.error_pos = p.lexpos


eqlparser = yacc.yacc()


def syntax_parse(data, debug=0):
    eqlparser.error = False
    eqlparser.error_p = None
    eqlparser.error_pos = -1
    r = eqlparser.parse(data, debug=debug)
    if eqlparser.error:
        r = None
    return r, eqlparser.error_p


if __name__ == '__main__':
    print(syntax_parse('\suggest add P495:_alias:origin \(country\)(_语种:en,factID:A$0a9c0becbcb4c2c0858a6d28e4bd67e0)', 0))
    print(syntax_parse('?x:所获奖项:?y(日期:1945)', 0))
