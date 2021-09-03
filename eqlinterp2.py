# An implementation of EQL, zhaohj@sina.com, 2020-6-20
#
import dbutils
import json
import time
from functools import reduce
import requests

from eqlparse import syntax_parse
from eqlrowset import Eqlrowset

# 底层数据库切换
import configparser
database_conf = configparser.ConfigParser()
database_conf.read('conf/database.conf')
underLyingDatabase = database_conf.get('db', 'underLyingDatabase')
if underLyingDatabase == 'elasticsearch':
    from eqldatabase import Eqldatabase
elif underLyingDatabase == 'sqlite':
    from eqldatabase_sqlite import Eqldatabase

from eqldataset import rowset_join
from eqlutils import utils
from han import f2j, get_lang

eqlengine_conf = configparser.ConfigParser()
eqlengine_conf.read('conf/eqlengine.conf')

def jian(s):
    return f2j(s)

class EqlInterpreter:
    waiting_audit=True
    pass_the_audit=False
    def __init__(self, db):
        self.data = ''
        self.client_ip = ''
        self.program = ''
        self.start_time = time.time()
        self.rowsets = []
        self.answer = "answer working"
        self.tmp_var_index = 0
        self.db_name = db
        self.db = Eqldatabase(eqlengine_conf.get('es','host'), eqlengine_conf.get('es','port'), eqlengine_conf.get('es','user'), eqlengine_conf.get('es','pwd'), db)
        self.ut = utils()
        self.arg_string = []  # of current program
        self.arg_var = []  # of current statement
        self.lang = ''
        self.var_result = {}
        self.to_groupby = {}
        self.ans_arg = None
        self.ans_dup = []
        self.ans_sim = []
        self.current_eql = ''
        self.status = ''
        self.progress = []
        self.is_final = False
        self.stopped = False
        self.limit = 0
        self.output_count = 0

    def simple_sp(self, s, p):
        lang = get_lang(s + p)
        recs = self.db.search_simple([dbutils.label2wid(self.db,s)], [dbutils.label2wid(self.db,p)], '')
        answer = []
        for rec in recs:
            match = True
            if p == '_label':
                match = lang == rec['_source']['qv']['_lang']
            if match:
                o = dbutils.widx2label(self.db, rec['_source']['o'], lang)
                answer.append(o)
        if len(answer) == 1:
            ans = {'answer': answer[0]}
        else:
            ans = {'answer': answer}
        return json.dumps(ans, ensure_ascii=False)

    @staticmethod
    def alter(rowset, value, values):
        rowset2 = []
        for row in rowset:
            if row[0][2] == value:
                for v in values:
                    row2 = ((row[0][0], '', v),)
                    for col in row[1:]:
                        row2 += (col,)
                    rowset2 += [row2]
            else:
                rowset2 += [row]
        return rowset2

    @staticmethod
    def change_var_name( rowset, var_name):
        if len(rowset) > 0:
            if len(rowset[0]) >0:
                return list(map(lambda r: ((var_name, r[0][1], r[0][2]),), rowset))
        return []

    def get_all_vars(self):
        s = []
        for rowset in self.rowsets:
            for row in rowset:
                for col in row:
                    if len(col[0]) > 0 and (col[0][0] == '?' or col[0][0] == '？'):
                        if not col[0] in s:
                            s.append(col[0])
        return s

    def get_var_values(self, rowset, var_name):
        # rowset_all = reduce(lambda x, y: x + y, self.rowsets, [])
        list1 = []
        if len(rowset) > 0:
            # if var_name in self.get_all_vars():
            er = Eqlrowset(self.db, rowset)
            rowset1 = er.get_column(var_name)
            if len(rowset1) > 0:
                list1 = list(map(lambda t: t[0][2], rowset1))
        return list1
    #
    # def get_lang(self,formal):
    #     n = reduce( lambda x,y : x+(0,1)[ord(y) in range(97,122)], formal.lower(), 0)
    #     return ('zh','en')[n*2 > len(formal)]
    #
    # def change_lang2(self,prop_expr):
    #     ss = prop_expr.split('.')
    #     ss = list(map(lambda x:self.change_lang(x), ss))
    #     return '.'.join(ss)
    #
    # def change_lang3(self,prop_expr):
    #     ss = prop_expr.split(',')
    #     ss = list(map(lambda x:self.change_lang2(x), ss))
    #     return ','.join(ss)
    #
    # def change_lang(self,formal):
    #     f = formal
    #     if self.get_lang(f) != self.lang:
    #         r1 = self.db.search2(('string',[formal]), ('string',['缺省id']), ('var',['?x']))
    #         if len(r1) > 0:
    #             if len(r1[0][0]) > 2:
    #                 id = r1[0][0][2]
    #                 r2 = self.db.search2(('var', ['?x']), ('string', ['缺省id']), ('string', [id]))
    #                 for r in r2:
    #                     if len(r[0]) > 2:
    #                         if r[0][2] != formal:
    #                             f = r[0][2]
    #     return f

    def register_arg(self, args):
        for arg in args:
            if len(arg) >= 2:
                if arg[0] == 'var' or arg[0] == 'var_string':
                    if arg[1].startswith('?') or arg[1].startswith('？'):
                        if arg[1] not in self.arg_var:
                            self.arg_var.append(arg[1])
                if arg[0] == 'string':
                    if arg[1] not in self.arg_string:
                        self.arg_string.append(arg[1])

    def get_2nd(self, arg, to_wid=True):
        t = ''
        res = []
        if len(arg) >= 2:
            if arg[0] == 'var' or arg[0] == 'var_string':
                t = arg[1]
                values = self.var_result.get(t)
                if values is not None:
                    res = values
                else:
                    res = []
            if arg[0] == 'string':
                t = ''
                if to_wid:
                    res = [dbutils.label2wid(self.db, arg[1])]
                else:
                    res = [arg[1]]
            if arg[0] == 'string_string':
                t = ''
                recs = self.db.search_simple([dbutils.label2wid(self.db, arg[1])], [dbutils.label2wid(self.db, arg[2])], '')
                res = []
                for rec in recs:
                    res.append(rec['_source']['o'])
        return t, res

    def get_tmp_var(self):
        self.tmp_var_index += 1
        return '?t'+str(self.tmp_var_index)

    def replace_qmark(self, var):
        if len(var) == 2 and var[0] == 'var' and (var[1] == '?' or var[1]=='？'):
            return var[0],self.get_tmp_var()
        elif len(var) == 3 and var[0] == 'var_string' and (var[1] == '?' or var[1] == '？'):
            return var[0], self.get_tmp_var(),var[2]
        else:
            return var

    def callback(self, i, total, r):
        #print('callback: ',i,total,r)
        if self.status == 'done':
            return
        if self.stopped:  #todo
            self.progress = []
            self.rowsets = []
            #time.sleep(300)
            return

        total2 = 100
        if total != 0:
            percent = int(i*total2/total)
            if percent > total2:
                percent = total2
        else:
            percent = 0
        rec_j = []
        s = 'running'
        if self.is_final and self.apply_var_result_record(r):
            if r is not None and r != '':
                s = 'output'
                rec = self.pro_answer_record(r)
                for col in rec:
                    col_j = {}
                    col_j["var"] = dbutils.propexpr2label(self.db, col[0], self.lang)
                    col_j["id"] = col[2]
                    col_j["label"] = dbutils.widx2label(self.db, col[2], self.lang)
                    rec_j.append(col_j)
        rec_str = json.dumps(rec_j, ensure_ascii=False)

        if rec_str != '[]':
            for p in self.progress:  # 重复结果不存,不适合所有情形
                if p['r'] == rec_str:
                    rec_j = []
                    rec_str = json.dumps(rec_j, ensure_ascii=False)
                    break

        if not ( s == 'output' and rec_str=='[]' ):
            r = {"s":s,"e":self.current_eql, "i":str(percent), "n":str(total2),"r":rec_str}
            if len(self.progress) == 0 or r != self.progress[-1]:
                self.progress.append(r)
                if s == 'output':
                    self.output_count += 1
                    if (self.limit > 0) and (self.output_count >= self.limit):
                        s = 'done'
                        self.status = s
                        res = {"s": s, "e": "", "i": "", "n": "", "r": ""}
                        self.progress.append(res)
        self.status = s

    def pro_answer_record(self,r):
        rowset = [r]
        if self.ans_arg is None:
            all_var = reduce(lambda x, y: x + [y[0]], rowset[0], [])
            arg = ','.join(all_var)
        else:
            arg = self.ans_arg[0]
        new_rowset = self.ut.print_to_rowset(self.db, rowset, arg)
        return new_rowset[0]

    def pro_repeat_s_var(self, s=('',[]), p=('',[]), o=('',[]), r=(0, 0)):
        rowset_all = []
        rowset_last = []
        for i in range(0,int(r[1])+1):
            hit = i >= int(r[0])
            rowset_this = []
            if i == 0:
                for ox in o[1]:
                    row = ((s[0], '', ox),)
                    rowset_this.append(row)
                    if hit:
                        self.callback(1,len(o[1]),row)
            elif i >= 1:
                o1 = []
                for row in rowset_last:
                    o1.append(row[0][2])
                if len(o1) > 0:
                    if hit:
                        rowset_this = self.db.search2(s, p, ('', o1), callback=self.callback)
                    else:
                        rowset_this = self.db.search2(s, p, ('', o1), callback=None)
                else:
                    rowset_this = []
            if hit:
                rowset_all += rowset_this
            rowset_last = rowset_this
        return rowset_all

    def pro_repeat_o_var(self, s=('', []), p=('', []), o=('', []), r=(0, 0)):
        rowset_all = []
        rowset_last = []
        for i in range(0,int(r[1])+1):
            hit = i >= int(r[0])
            rowset_this = []
            if i == 0:
                for sx in s[1]:
                    row = ((o[0], '', sx),)
                    rowset_this.append(row)
                    if hit:
                        self.callback(1,len(s[1]),row)
            elif i >= 1:
                s1 = []
                for row in rowset_last:
                    s1.append(row[0][2])
                if s1 != []:
                    if hit:
                        rowset_this = self.db.search2(('', s1), p, o, callback=self.callback)
                    else:
                        rowset_this = self.db.search2(('', s1), p, o, callback=None)
                else:
                    rowset_this = []
            if hit:
                rowset_all += rowset_this
            rowset_last = rowset_this
        return rowset_all

    def pro_repeat_s_o_var(self, s=('', []), p=('', []), o=('', []), r=(0, 0)):
        rowset_all = []
        rowset_last = []
        for i in range(1,int(r[1])+1):
            hit = i >= int(r[0])
            rowset_this = []
            if i == 1:
                is_final = self.is_final
                self.is_final = self.is_final and hit
                rowset_this = self.db.search2(s, p, o, callback=self.callback)
                rowset_one = rowset_this
                self.is_final = is_final
            elif i >= 1:
                rowset_this = []
                k = 1
                n = len(rowset_one) * len(rowset_last)
                for row in rowset_last:
                    for row_one in rowset_one:
                        r1 = None
                        if row[1][2] == row_one[0][2]:
                            r1 = (row[0],row_one[1])
                        if row[0][2] == row_one[1][2]:
                            r1 = (row_one[0], row[1])
                        if r1 is not None:
                            if r1 not in rowset_this:
                                rowset_this.append(r1)
                                if hit:
                                    self.callback(k, n, r1)
                        k += 1
            if hit:
                rowset_all += rowset_this
            rowset_last = rowset_this
        return rowset_all

    def pro_repeat(self, s=('', []), p=('', []), o=('', []), r=(0, 0)):
        if s[0] != '' and p[0] == '' and o[0] == '':
            return self.pro_repeat_s_var(s,p,o,r)
        if s[0] == '' and p[0] == '' and o[0] != '':
            return self.pro_repeat_o_var(s,p,o,r)
        if s[0] != '' and p[0] == '' and o[0] != '':
            return self.pro_repeat_s_o_var(s,p,o,r)
        # if s[0] == 'var' and p[0] == 'string' and o[0] == 'string':
        #     return self.pro_repeat_s_var(s,p,o,r)
        # if s[0] == 'string' and p[0] == 'string' and o[0] == 'var':
        #     return self.pro_repeat_o_var(s,p,o,r)
        # if s[0] == 'var' and p[0] == 'string' and o[0] == 'var':
        #     return self.pro_repeat_s_o_var(s,p,o,r)
        return []

    def pro_sp(self, sp):
        pass #todo 别名
        rowset = []
        if len(sp) == 1:
            s = self.get_2nd(('string',sp[0]))
            p = self.get_2nd(self.replace_qmark(('var','?')))
            o = self.get_2nd(self.replace_qmark(('var','?')))
            rowset = self.db.search2(s, p, o, callback=self.callback, need_factid=True)
        elif len(sp) >= 2:
            for i in range(1,len(sp)):
                if i == 1:
                    s = self.get_2nd(('string',sp[0]))
                else:
                    sx = []
                    for row in rowset:
                        sx += self.get_2nd(('string',row[0][2]))[1]
                    s = ('', sx)
                p = self.get_2nd(('string', sp[i]))
                o = self.get_2nd(self.replace_qmark(('var', '?')))
                if i == len(sp)-1:
                    rowset = self.db.search2(s, p, o, callback=self.callback)
                else:
                    rowset = self.db.search2(s, p, o, callback=None)
        return rowset

    def pro_fact(self, args, is_final, need_factid):
        # print('arg_fact=',args)
        if args[0][0] == 'sp':
            self.current_eql = 'spo:' + '.'.join(args[0][1:])  #todo 统一在search2之前处理？
            self.is_final = is_final
            return self.pro_sp(args[0][1:])

        spo_str = args[0][1][1] + ':' + args[0][2][1] + ':' + args[0][3][1]
        s = self.replace_qmark(args[0][1])
        p = self.replace_qmark(args[0][2])
        o = self.replace_qmark(args[0][3])
        self.register_arg([s,p,o])
        s2 = self.get_2nd(s)
        p2 = self.get_2nd(p)
        o2 = self.get_2nd(o)
        qvlist = []
        if len(args) == 2:
            for qv in args[1][1:]:
                q = ('','')
                v = ('','')
                if qv[0] == 'qv':  # <class 'tuple'>: (('qv', ('string', '日期'), ('var', '?x')), ('qv', ('string', '獎金'), ('var', '?y')))
                    q = self.replace_qmark(qv[1])
                    v = self.replace_qmark(qv[2])
                    spo_str += '(' + qv[1][1] + ':' + qv[2][1] + ')'
                if qv[0] == 'var':
                    q = self.replace_qmark(qv)
                    v = self.replace_qmark(qv)
                    spo_str += '(' + qv[1] + ')'
                self.register_arg([q, v])
                q2 = self.get_2nd(q)
                to_wid = True
                if len(q2) >= 2:
                    to_wid = 'P585' not in q2[1] and '检索词' not in q2[1] and 'searchfor' not in q2[1]
                v2 = self.get_2nd(v, to_wid)
                qvlist.append((q2, v2))
        self.current_eql = spo_str
        rowset = []
        self.is_final = is_final and s[0] != 'var_string'
        self.callback(0, 0, '')
        if args[0][0] == 'spo':
            rowset += self.db.search2(s2,p2,o2,qvlist, callback=self.callback, need_factid=need_factid)
        if args[0][0] == 'repeat':
            rowset += self.pro_repeat(s2,p2,o2,(int(args[0][4][1]),int(args[0][4][2])))

        if len(rowset) > 0:
            if s[0] == 'var_string':
                er = Eqlrowset(self.db, rowset)
                rowset1 = er.get_column(s[1])
                #self.current_eql = 'spo'
                self.is_final = False

                rs = []
                for r in rowset1:
                    r1 = r[0][2]  # self.get_2nd(('string',r[0][2]))
                    if len(r1) > 0:
                        rs.append(r1)
                s2 = self.get_2nd(s)
                p2 = self.get_2nd(('string',s[2]))
                self.register_arg([('string',s[2])])
                t_var = self.get_tmp_var()
                o2 = (t_var,rs)
                rowset2 = self.db.search2(s2,p2,o2,callback=self.callback)
                # list2 = list(map(lambda t: t[0][2], rowset2))
                i = 1
                for r in rs:
                    list3 = []
                    for r3 in rowset2:
                        if r3[1][2] == r:
                            list3.append(r3[0][2])
                    rowset = self.alter(rowset, r, list3)
                    self.callback(i, len(rs), '')
                    i += 1
                # i = 1
                # for r in rowset1:
                #     s2 = self.get_2nd(s)
                #     p2 = self.get_2nd(('string',s[2]))
                #     self.register_arg([('string',s[2])])
                #     o2 = self.get_2nd(('string',r[0][2]))
                #     rowset2 = self.db.search2(s2,p2,o2)
                #     list2 = list(map(lambda t: t[0][2], rowset2))
                #     rowset = self.alter(rowset, r[0][2], list2)
                #     self.callback(i,len(rowset1),'')
                #     i += 1
                #self.current_eql = 'spo'
                self.is_final = is_final
                i = 1
                for r in rowset:
                    self.callback(i, len(rowset), r)
                    i += 1
        return rowset

    def pro_compare(self, rowset, args, is_final):
        # print('arg_compare=',args)
        rowset2 = []
        if len(rowset) > 0:
            self.current_eql = '\\filter'
            self.is_final = is_final
            self.callback(0,0,'')
            er = Eqlrowset(self.db, rowset)
            if args[0][0] == 'function':
                rowset2 = er.filter_func(args[0][1], dbutils.propexpr2wid(self.db, args[0][2][1]), args[0][3],args[1],args[2])
            if args[0][0] == 'prop_expr':
                if args[1] == '\\match':
                    rowset2 = er.filter_match(dbutils.propexpr2wid(self.db, args[0][1]),args[2],self.lang,callback=self.callback)
                else:
                    wid = dbutils.propexpr2wid(self.db, args[2])
                    prop = self.simple_sp(args[2], '性质')
                    if '年' in json.loads(prop)['answer']:
                        label = dbutils.widx2label(self.dbwid,self.lang)
                        value = label.replace('年', '')
                    else:
                        value = wid
                    rowset2 = er.filter_prop(dbutils.propexpr2wid(self.db, args[0][1]),args[1],value,callback=self.callback)
        return rowset2

    def contains(self, s, ss):
        if len(ss) > 0 and isinstance(ss[0],tuple):
            for t in ss:
                if s in t:
                    return True
        return s in ss

    def apply_var_result(self, rowset):
        rowset2 = []
        for row in rowset:
            if self.apply_var_result_record(row):
                rowset2 += [row]
        return rowset2

    def apply_var_result_record(self, row):
            row_ok = True
            if row is not None:
                for col in row:
                    res = self.var_result.get(col[0])
                    if res is not None:
                        row_ok = row_ok and self.contains(col[2], res)
            return row_ok

    def pro_query_list(self, args, is_final):
        # print('arg_query_list=',args)
        rowset = []
        for q in args:
            rowset1 = []
            if len(q) >= 2:
                if q[1][0] == 'fact':
                    rowset1 = self.pro_fact(q[1][1:], is_final and len(args)==1, len(args)==1)
                if q[1][0] == 'compare':
                    rowset1 = self.pro_compare(rowset, q[1][1:], False)
            if len(rowset) == 0:
                rowset += rowset1
            else:
                if len(rowset) > 0:
                    is_final2 = is_final and q == args[len(args) - 1]
                    self.is_final = is_final2
                    er = Eqlrowset(self.db, rowset)
                    if q[0] == '\\and':
                        self.current_eql = q[0]
                        self.callback(0, 0, '')
                        #rowset = er.intersect(rowset1,callback=self.callback)
                        rowset = rowset_join(rowset, rowset1, self.callback)
                    if q[0] == '\\or':
                        self.current_eql = q[0]
                        self.callback(0, 0, '')
                        rowset = er.union(rowset1,callback=self.callback)
                    if q[0] == '\\not':
                        self.current_eql = q[0]
                        self.callback(0, 0, '')
                        rowset = er.exclude(rowset1,callback=self.callback)
        rowset = self.apply_var_result(rowset)
        return rowset

    def pro_query(self, args):
        self.limit = 1000
        for arg in args:
            if isinstance(arg,tuple):
                if len(arg) >= 2:
                    if arg[0] == 'limit':
                        if arg[1].isdigit():
                            self.limit = int(arg[1])

        rowsets1 = [self.pro_query_list(args[0], args[1] is None and args[2] is None and args[3] is None)]
        rowsets2 = rowsets1

        arg = args[1]
        if arg is not None:
            if arg[0] == 'filter_list':
                for f in arg[1:]:
                    rowsets1 = [self.pro_compare(rowsets1[0],f[2][1:], args[2] is None and args[3] is None
                                                 and f == arg[len(arg)-1])]
                    rowsets2 = rowsets1

        arg = args[2]
        if arg is not None:
            if arg[0] == 'groupby':
                er = Eqlrowset(self.db, rowsets1[0])
                self.current_eql = arg[0]
                self.is_final = args[3] is None
                self.callback(0, 0, '')
                rowsets1x = er.groupby(dbutils.propexpr2wid(self.db, arg[2][1]),callback=self.callback)
                rowsets1 = list(reduce( lambda x,y: x+y, rowsets1x, [] ))
                rowsets2 = [list(reduce( lambda x,y: x+y, rowsets1, [] ))]

        arg = args[3]
        if arg is not None:
            if arg[0] == 'orderby':
                for i in range(0,len(rowsets1)):
                    if len(rowsets1[i]) > 0:
                        er = Eqlrowset(self.db, rowsets1[i])
                        order = arg[3]
                        if order is None:
                            order = 'asc'
                        self.current_eql = arg[0]
                        self.is_final = True
                        self.callback(0, 0, '')
                        rowsets1[i] = er.orderby(arg[2][1],order,callback=self.callback)
                rowsets2 = [list(reduce(lambda x, y: x + y, rowsets1, []))]
        self.rowsets += rowsets2

    def pro_compute(self, args):
        # print('arg_compute=',args)
        rowset2 = []
        if self.rowsets is not None:
            for rowset in self.rowsets[::-1]:
                if len(rowset)>0:
                    er = Eqlrowset(self.db, rowset)
                    self.register_arg([('var', args[0])])
                    self.current_eql = 'compute'
                    self.is_final = True
                    self.callback(0, 0, '')
                    rowset2 = er.function(args[0],args[1][1],args[1][2][1],args[1][3],callback=self.callback)
                    if len(rowset2) > 0:
                        break
        rowset2 = self.change_var_name(rowset2, args[0])
        self.rowsets += [rowset2]

    def pro_answer2(self):
        # print('arg_answer=',args)
        ans = []
        for rowset in self.rowsets:
            if len(rowset) >0 and len(rowset[0])>0:
                if self.ans_arg is None:
                    all_var = reduce(lambda x, y: x + [y[0]], rowset[0], [])
                    arg = ','.join(all_var)  # get_all_vars())
                else:
                    arg = self.ans_arg[0]
                try:
                    ans.append(self.ut.print_to_rowset(self.db, rowset, arg, self.lang))
                except:
                    ans.append(rowset)
            else:
                ans.append(rowset)
        self.answer = ans

    def pro_statement(self, statement):
        # print('arg_statement=',statement)
        self.arg_var = []
        if len(statement) >= 2:
            if statement[0] == 'query':
                self.pro_query(statement[1:])
            if statement[0] == 'compute':
                self.pro_compute(statement[1:])
            if statement[0] == 'answer':
                self.ans_arg = statement[1:]
                # self.pro_answer(statement[1:])
        for arg in self.arg_var:
            if self.var_result.get(arg) is None:
                self.var_result[arg] = self.get_var_values(self.rowsets[-1],arg)

    def get_des(self,arg):
        rowset = self.db.search2(('',[arg]),('',['des']),('?x',[]))
        if rowset:
            return rowset[0][0][2]
        else:
            return arg

    # ((('db', ('ADD', ('data', ('data_spo', '欧内斯特·海明威', '主要作品', '《流动的圣节》'), ('data_qv', ('首次出版', '1964年')))),
    #    ('REF', ('《海明威研究文集》 ISBN 978-7-5447-3164-5', '维基百科 www.baidu.com'))),), None)

    def data2json(self, args):#args(('data_spo', '欧内斯特·海明威', '主要作品', '《流动的圣节》'), ('data_qv', ('首次出版', '1964年'), ('出版社', 'Simon & Schuster')))
        #print('进入2json\n', args)
        id = None
        if len(args) >= 1:
            s2 = dbutils.label2wid(self.db, args[0][1])
            p2 = dbutils.label2wid(self.db, args[0][2])
            if p2 != '_freq' and p2 != '_alias' and p2 != '_label':
                o2 = dbutils.label2wid(self.db, args[0][3])
            else:
                o2 = args[0][3]
            text = [{"s": s2, "p": p2, "o": o2}]
            if len(args) == 2:
                qv = {}
                for i in range(1,len(args[1])):
                    q2 = dbutils.label2wid(self.db, args[1][i][0])
                    if q2 != '_des' and q2 != '_lang':
                        v2 = dbutils.label2wid(self.db, args[1][i][1])
                    else:
                        v2 = args[1][i][1]
                    if q2 == 'factID':
                        id = v2
                    else:
                        qv[q2] = v2
                text[0]['qv'] = qv
            else:
                text[0]['qv'] = {}
        #print('json', text)
        return text, id

    def pro_add(self,args):
        #print('进入add')
        text, id = self.data2json(args[1])
        if len(text) > 0:
            if not str(text[0].get('p')).startswith('_'):
                self.db.load_form_text(text,id)
                #print("已插入")

    def pro_change(self,args):
        text0, _ = self.data2json(args[0][1])
        text1,_=self.data2json(args[1][1])
        id = self.db.search_id(text0)
        if len(id)==1:
            self.db.load_form_text(text1, id[0])
            return 1
        else:
            return 0




    def pro_delete(self,args):
        text, _ = self.data2json(args[1])
        id = self.db.search_id(text)
        self.db.delete_by_id(id)


    def pro_db(self,args):
        if len(args)>=2:
            if args[0]=='add':
                self.pro_add(args[1])
            if args[0] == 'change':
                self.pro_change(args[1])
            if args[0]=='delete':
                self.pro_delete(args[1])


    def interpret(self, lang, args):
        # print('arg_program=', args)
        self.rowsets = []
        self.answer = ''
        self.tmp_var_index = 0
        self.arg_string = []
        self.lang = lang
        self.var_result = {}
        self.to_groupby = {}
        self.ans_arg = None
        self.ans_dup = []
        self.ans_sim = []
        self.current_eql = ''
        self.status = ''
        self.progress = []
        self.is_final = False

        for statement in args:
            #print(statement)
            if len(statement) >= 2:
                if statement[0] == 'statement':
                    if statement[1][0] == 'answer':
                        self.ans_arg = statement[1][1:]

        for statement in args:
            #print(statement,args)
            if len(statement) >= 2:
                if statement[0] == 'statement':
                    self.pro_statement(statement[1])
                else:
                    #print('处理db操作')
                    while self.waiting_audit:
                        time.sleep(1)

                        #self.waiting_audit = False
                        #self.pass_the_audit = True

                        pass
                    if self.pass_the_audit:
                        #print('审核通过')
                        self.pro_db(statement[1])
                        #print('处理完毕')
                    else:
                        pass
                        #print('审核不通过')
        # self.pro_groupby()

    def run(self, ip, prog, lang, args):
        self.client_ip = ip
        self.program = prog
        self.start_time = time.time()
        self.interpret(lang,args)
        s = 'done'
        self.status = s
        res = {"s":s, "e":"", "i":"", "n":"", "r":""}
        self.progress.append(res)

    def get_similar(self,arg):
        res = []
        # sim = self.ut.get_similar(self.db,arg,0.1)
        # if isinstance(sim,list):
        #     sim = sorted(sim,key=lambda x:x[1], reverse=True)
        #     if not (len(sim) == 1 and sim[0][0] == arg):
        #         for s in sim:
        #             res.append({'sim':s[0],'rate':s[1]})
        return res

    def get_des(self,arg):
        records = self.db.search_simple('', ['_label'], [arg])
        des = ''
        for rec in records:
            des = f2j(rec['_source']['qv']['_des'])
        return {"des":des}
        # rowset = self.db.search2(('string',[arg]),('string',['des']),('var',['?x']))
        # if rowset:
        #     return {"des":rowset[0][0][2]}
        # else:
        #     return {"des":arg}

    def get_duplicate(self,arg):
        res = []
        #dups = self.ut.get_names(self.db,arg)
        dups = dbutils.label2dup(self.db, arg)
        if isinstance(dups,list):
            for s in dups:
                 res.append({'dup':s})
        return res


eqls = {}
def eql_interpret(ip,db, data,lang,eql_id=''):
        eql = EqlInterpreter(db)
        eql.data = data
        eqls[eql_id] = eql

        if lang is None or lang == '':
            lang = 'zh'
        r, error_p = syntax_parse(data, debug=0)
        if r is not None and error_p is None:
            eql.run(ip,data,lang,r)
        else:
            s = 'error'
            eql.status = s
            eql.program = data
            eql.client_ip = ip
            eql.lang = lang
            res = {'s': s, 'e': data, 'i': '', 'n': '', 'r': '{"pos":' + str(error_p.lexpos)+'}'}
            eql.progress.append(res)

def eql_interpret_result(eql_id,istart):
    res = []
    eql = eqls.get(eql_id)
    if eql is not None:
        n = len(eql.progress)
        done = False
        for i in range(istart,min(istart+100,n)):
            if eql.progress[i]['s'] == 'done':
                done = True
            res.append(eql.progress[i])
        if done:
            delete_eql(eql_id)
    else:
        res = [{"s":"invalid", "e":"", "i":"", "n":"", "r":""}]
    #print("res",res)
    return res

def get_eql_count():
    return {"count":len(eqls)}

def get_eql(index):
    r = None
    i = 0
    for t in eqls:
        if i == index:
            eql = eqls[t]
            r = {"eql_id":t,"ip": eql.client_ip, "eql": eql.program, "duration": int((time.time() - eql.start_time)*1000),
                 "status:": eql.status}
            break
        i += 1
    return r

def get_eqls():
    r = []
    for k in eqls.keys():
        eql = eqls[k]
        r.append({"eql_id":k,"ip": eql.client_ip, "db":eql.db_name, "lang":eql.lang, "eql": eql.program, "duration": int((time.time() - eql.start_time)*1000),
                 "status:": eql.status})
    return r

def delete_eql(eql_id):
    if eqls.get(eql_id) is not None:
        eqls[eql_id].stopped = True
        del eqls[eql_id]
        return {"accepted":"true"}
    else:
        return {"accepted":"false"}

def eql_interpret_audit_get():
    return {"eql":"?:?:?"} #todo

def eql_interpret_audit(eql_id,yn):
    eql = eqls.get(eql_id)
    #print(eqls)
    if eql is not None:
        res = {"accepted":"true"}  #todo
        if yn=="yes":
            eql.waiting_audit = False
            eql.pass_the_audit=True
        elif yn=="no":
            eql.waiting_audit = False
            eql.pass_the_audit = False
    else:
        res = {"accepted": "false"}
    return res


def eql_db_post_delete(dbname,op):
    ret = False

    #print("进入eql_db_post_delete", dbname, op)
    if dbname in ['nobeloscars', 'nobeloscars_backup', 'davinci', 'davinci_8e', 'eqldb_log', 'eqldb_db', 'eqldb_db_log']:
        return ret

    if underLyingDatabase == 'elasticsearch':
        baseurl = 'http://' + eqlengine_conf.get('es', 'host') + ':' + eqlengine_conf.get('es', 'port') + '/'
        user = eqlengine_conf.get('es', 'user')
        pwd = eqlengine_conf.get('es', 'pwd')

        if op == 'delete':
            url = baseurl + dbname
            headers = {'content-type': 'application/json'}
            query_str = json.dumps({})
            response = requests.delete(url, auth=(user,pwd), data=query_str.encode(encoding='utf-8'), headers=headers)
            result = json.loads(response.text)
            ret = result.get('acknowledged') is True

        if op == 'post':
            url = baseurl + dbname
            headers = {'content-type': 'application/json'}
            query_str = json.dumps({})
            response = requests.put(url, auth=(user,pwd), data=query_str.encode(encoding='utf-8'), headers=headers)
            result = json.loads(response.text)
            ret = result.get('acknowledged') is True

            url = baseurl + dbname + '/_mapping'
            query_str = json.dumps({
                "properties" : {
                    "s" : {
                        "type" : "keyword"
                    },
                    "p" : {
                        "type" : "keyword"
                    },
                    "o" : {
                        "type" : "keyword"
                    },
                    "qv" : {
                        "type": "flattened"
                    }
                }
            }
            )
            response = requests.post(url, auth=(user,pwd), data=query_str.encode(encoding='utf-8'), headers=headers)
            result = json.loads(response.text)
            ret = ret and result.get('acknowledged') is True

            url = baseurl + dbname + '/_settings'
            query_str = json.dumps({
                "index": {
                    "max_result_window": 500000,
                    "refresh_interval" : "100ms"
                }
            })
            response = requests.put(url, auth=(user,pwd), data=query_str.encode(encoding='utf-8'), headers=headers)
            result = json.loads(response.text)
            ret = ret and result.get('acknowledged') is True

    elif underLyingDatabase == 'sqlite':
        if op == 'delete':
            import os
            try:
                os.remove('./dbs/' + dbname + '.db')
                ret = True
            except:
                ret = False
        if op == 'post':
            from sqlalchemy import create_engine
            from sqlalchemy.ext.declarative import declarative_base
            from sqlalchemy import Column, Text, ForeignKey, Index
            from sqlalchemy.orm import relationship

            Base = declarative_base()

            class SPO(Base):
                __tablename__ = 'spo'
                factID = Column(Text, primary_key=True, index=True)
                s = Column(Text, index=True)
                p = Column(Text, index=True)
                o = Column(Text, index=True)

                qvs = relationship('QV', backref='spo', lazy='dynamic')

            class QV(Base):
                __tablename__ = 'qv'
                factID = Column(Text, ForeignKey("spo.factID"), index=True, primary_key=True)
                q = Column(Text, index=True, primary_key=True)
                v = Column(Text, index=True, primary_key=True)

            Index('idx_spo_id_s', SPO.factID, SPO.s)
            Index('idx_spo_id_p', SPO.factID, SPO.p)
            Index('idx_spo_id_o', SPO.factID, SPO.o)
            Index('idx_spo_s_p', SPO.s, SPO.p)
            Index('idx_spo_s_o', SPO.s, SPO.o)
            Index('idx_spo_p_o', SPO.p, SPO.o)

            Index('idx_spo_id_s_p', SPO.factID, SPO.s, SPO.p)
            Index('idx_spo_id_s_o', SPO.factID, SPO.s, SPO.o)
            Index('idx_spo_id_p_o', SPO.factID, SPO.p, SPO.o)
            Index('idx_spo_s_p_o', SPO.s, SPO.p, SPO.o)

            Index('idx_spo_id_s_p_o', SPO.factID, SPO.s, SPO.p, SPO.o)

            Index('idx_qv_id_q', QV.factID, QV.q)
            Index('idx_qv_id_v', QV.factID, QV.v)
            Index('idx_qv_q_v', QV.q, QV.v)
            Index('idx_qv_id_q_v', QV.factID, QV.q, QV.v)

            try:
                engine = create_engine('sqlite:///dbs/' + dbname + '.db')
                Base.metadata.create_all(engine, checkfirst=True)
                ret = True
            except:
                ret = False
    return ret

if __name__ == '__main__':
    eql_interpret('127.0.0.1', 'davinci', '?:所获奖项:?(日期:?x, 獎金:?y)', 'zh', '1234')
