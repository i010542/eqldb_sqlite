# EQL lexer by zhaohj 2020-6-15
import json
from functools import reduce
from traceback import format_exc

from ply import *

keywords = (
)

tokens = keywords + (
    'STRING',
    'COLON', 'VAR', 'LPAREN', 'RPAREN',
    'EQ',
    'NOTEQ',
    'GT',
    'GE',
    'LT',
    'LE',
    'COMMA',
    'DOT',
    'AND',
    'OR',
    'NOT',
    'GROUPBY',
    'ORDERBY',
    'LIMIT',
    'UNLIMIT',
    'DESC',
    'ASC',
    'FILTER',
    'COUNT',
    'AVG',
    'SUM',
    'MAX',
    'MIN',
    'MATCH',
    'ANS',
    'OF',
    'ADD',
    'CHANGE',
    'DELETE',
    'CHANGETO',
    'REF',
    'ADDMARK',
    'SUBMARK',
    'REPEAT',
    'TILDE',
    'NUMBER'
)

t_ignore = ' \t\n\r'

# 正则里需要加\的字符
# $ ( ) * + . [ ] ? \ / ^ { } |

def t_VAR(t):
    r"""(?<!\\)(\?|？)([x|y|z][1-9]?[1-9]?[1-9]?)?"""
    if isinstance(t.value,str):
        t.value = t.value.replace('？','?')
    return t


t_COLON = r'(?<!\\)(:|：)'

t_STRING = r'(?=a)b'

t_LPAREN = r'(?<!\\)(\(|（)'

t_RPAREN = r'(?<!\\)(\)|）)'

t_EQ = r'(?<!\\)(=|＝)'
t_NOTEQ = r'(?<!\\)((!=)|(！＝))'
t_GT = r'(?<!\\)(>|＞)'
t_GE = r'(?<!\\)(>=)|(＞＝)'
t_LT = r'(?<!\\)<|＜'
t_LE = r'(?<!\\)((<=)|(＜＝))'

t_COMMA = r'(?<!\\)(,|，)'
t_DOT = r'(?<!\\)\.'
t_AND = r'\\and'
t_OR = r'\\or'
t_NOT = r'\\not'
t_GROUPBY = r'\\group\sby'
t_ORDERBY = r'\\order\sby'
t_LIMIT = r'\\limit'
t_UNLIMIT = r'\\unlimit'
t_FILTER = r'\\filter'
t_ADD=r'\\suggest\sadd'
t_CHANGE=r'\\suggest\schange'
t_DELETE=r'\\suggest\sdelete'
t_REF=r'\\ref\d*'
t_CHANGETO=r'\\changeTo'

t_ASC = r'''((?<=\\order\sby\s(\?|？).)asc)|((?<=\\order\sby\s(\?|？)..)asc)
            |((?<=\\order\sby\s(\?|？)...)asc)|((?<=\\order\sby\s(\?|？)....)asc)
            |((?<=\\order\sby\s(\?|？).....)asc)|((?<=\\order\sby\s(\?|？)......)asc)
            |((?<=\\order\sby\s(\?|？).......)asc)|((?<=\\order\sby\s(\?|？)........)asc)
            |((?<=\\order\sby\s(\?|？).........)asc)|((?<=\\order\sby\s(\?|？)..........)asc)
        '''
t_DESC = r'''((?<=\\order\sby\s(\?|？).)desc)|((?<=\\order\sby\s(\?|？)..)desc)
            |((?<=\\order\sby\s(\?|？)...)desc)|((?<=\\order\sby\s(\?|？)....)desc)
            |((?<=\\order\sby\s(\?|？).....)desc)|((?<=\\order\sby\s(\?|？)......)desc)
            |((?<=\\order\sby\s(\?|？).......)desc)|((?<=\\order\sby\s(\?|？)........)desc)
            |((?<=\\order\sby\s(\?|？).........)desc)|((?<=\\order\sby\s(\?|？)..........)desc)
        '''

t_COUNT = r'(?<=\=|＝|\(|（)count(?=\(|（)'
t_AVG = r'(?<=\=|＝|\(|（)avg(?=\(|（)'
t_SUM = r'(?<=\=|＝|\(|（)sum(?=\(|（)'
t_MAX = r'(?<=\=|＝|\(|（)max(?=\(|（)'
t_MIN = r'(?<=\=|＝|\(|（)min(?=\(|（)'
t_MATCH = r'\\match'
t_ANS = r'ANS(?=\s(\?|？))'
t_OF = r'of(?=\s(\?|？))'

t_REPEAT = r'\\repeat'
t_ADDMARK = r'((?<=\\repeat(:|：)\d)(\+|＋))|((?<=\\repeat(:|：)\d\d)(\+|＋))'  # \d+ lexer出错？
t_SUBMARK = r'((?<=\\repeat(:|：)\d)(\-|－))|((?<=\\repeat(:|：)\d\d)(\-|－))'
t_TILDE = r'((?<=\\repeat(:|：)\d)(~|～))|((?<=\\repeat(:|：)\d\d)(~|～))'
# t_NUMBER = r'[0-9]+' # 与STRING冲突


def t_error(t):
    print("Illegal character %s" % t.value[0])
    t.lexer.skip(1)


lexer = lex.lex(debug=0)


def lex_parse(data, debug=0):
    tokens = []
    lexer.input(data)
    while True:
        tok = lexer.token()
        if not tok:
            break  # No more input
        tokens.append(tok)
    try:
        res = reduce(lambda r, x : r + [({'type':x.type,'value':x.value,'lineno':x.lineno,'lexpos':x.lexpos})], tokens, [])
        # res = reduce(lambda r, x: r + [({'type': x.type})],tokens, [])
    except Exception:
        print(format_exc())
    return json.dumps(res)

if __name__ == '__main__':
    print(lex_parse('\suggest add P495:_alias:origin \(country\)(_语种:en,factID:A$0a9c0becbcb4c2c0858a6d28e4bd67e0)', 1))
