# -*- coding: utf-8 -*-
import json
import jieba
import re
from han import get_lang, f2j, j2f
from sqlalchemy.orm import sessionmaker
import time
from random import random
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Text, ForeignKey
from sqlalchemy.orm import relationship
import dbutils

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


def translate_system_p(p):
    return p


def generate_id():
    return str(time.time()) + str(random())


class Eqldatabase:
    def __init__(self, host, port, user, pwd, db):
        self.db = db
        self.Session = sessionmaker(bind=create_engine('sqlite:///dbs/' + db + '.db?check_same_thread=False'))

    def get_qv_by_factID(self, factID):
        session = self.Session()
        qvs = session.query(QV.q, QV.v).filter_by(factID=factID).all()
        # session.close()
        qvs_list = []
        qvs_dict = {}
        for qv in qvs:
            if qv[0] == '日期' or qv[0] == 'P585':
                if re.search("^[0-9]*$", qv[1]):
                    qvs_list.append((qv[0], qv[1]))
                    qvs_dict[qv[0]] = qv[1]
                elif re.search(r"\d+-\d+-\d+", qv[1]):
                    year = re.findall(r"\d+\.?\d*", qv[1])[0]
                    qvs_list.append((qv[0], year))
                    qvs_dict[qv[0]] = year
            else:
                qvs_list.append((qv[0], qv[1]))
                qvs_dict[qv[0]] = qv[1]
        return qvs_list, qvs_dict

    def search_simple(self, s='', p='', o=''):
        session = self.Session()
        SQL = """SELECT DISTINCT SPO.factID, SPO.s, SPO.p, SPO.o FROM SPO\n"""
        SQL += "WHERE\n"
        if s != '':
            SQL += "SPO.s IN (" + str(s)[1: -1] + ") AND\n"
        if p:
            SQL += "SPO.p IN (" + str(p)[1: -1] + ") AND\n"
        if o:
            SQL += "SPO.o IN (" + str(o)[1: -1] + ") AND\n"
        if not s and not p and not o:
            SQL = SQL[0: -6]
        else:
            SQL = SQL[0: -4]
        cursor = session.execute(SQL)
        # session.close()
        result = cursor.fetchall()
        record = []
        for r in result:
            factID = r[0]
            s = r[1]
            p = r[2]
            o = r[3]
            _, qv = self.get_qv_by_factID(factID)
            record.append(
                {
                    '_index': self.db,
                    '_id': factID,
                    '_source': {
                        's': s,
                        'p': p,
                        'o': o,
                        'qv': qv
                    }
                }
            )
        return record

    def search(self, s, p, o, qvList, sizePage=1000, index=1, scroll_id=None):
        recordList = []
        session = self.Session()

        legalQVList = []

        for qv in qvList:
            q = qv[0][1]
            v = qv[1][1]
            if v == []:
                pass
            else:
                for i in range(0, len(q)):
                    if q[i] != '日期' and q[i] != 'P585':
                        legalQVList.append((q[i], v[i], 0))
                    else:
                        year = None
                        if re.search("^[0-9]*$", v[i]):
                            year = v[i]
                        elif re.search(r"\d+-\d+-\d+", v[i]):
                            year = re.findall(r"\d+\.?\d*", v[i])[0]
                        elif (len(v[i]) > 0 and v[i][0] >= '0' and v[i][0] <= '9'):  # 数值 单位 写法
                            year = re.findall(r"\d+\.?\d*", v[i])[0]
                        legalQVList.append((q[i], year, 1))
        legalQVListLength = len(legalQVList)
        SQL = """SELECT DISTINCT SPO.factID, SPO.s, SPO.p, SPO.o FROM SPO\n"""
        for i in range(legalQVListLength):
            SQL += "LEFT JOIN QV AS QV" + str(i + 1) + " ON (SPO.factID = QV" + str(i + 1) + ".factID)\n"
        SQL += "WHERE\n"
        if s[1]:
            SQL += "SPO.s IN (" + str(s[1])[1: -1] + ") AND\n"
        if p[1]:
            SQL += "SPO.p IN (" + str(p[1])[1: -1] + ") AND\n"
        if o[1]:
            SQL += "SPO.o IN (" + str(o[1])[1: -1] + ") AND\n"
        for i, qv in enumerate(legalQVList):
            SQL += "QV" + str(i + 1) + ".q='" + str(qv[0]) + "' AND\n"
            if qv[2] == 1:
                SQL += "QV" + str(i + 1) + ".v LIKE '" + str(qv[1]) + "%' AND\n"
            else:
                SQL += "QV" + str(i + 1) + ".v='" + str(qv[1]) + "' AND\n"

        if not s[1] and not p[1] and not o[1] and not legalQVList:
            SQL = SQL[0: -6]
        else:
            SQL = SQL[0: -4]

        SQL += " LIMIT " + str(sizePage) + " OFFSET " + str((index - 1) * sizePage)
        cursor = session.execute(SQL)
        # session.close()
        result = cursor.fetchall()
        for r in result:
            factID = r[0]
            s = r[1]
            p = r[2]
            o = r[3]
            qv, _ = self.get_qv_by_factID(factID)
            recordList.append([factID, s, p, o, qv])
        return recordList, len(recordList)

    def search2(self, s=('', []), p=('', []), o=('', []), qvlist=[], callback=None, need_factid=False):
        print("123123", s, p, o, qvlist)
        my_rowset = []
        sizePage = 10
        index = 1
        while True:
            record_list, length = self.search(s, p, o, qvlist, sizePage, index)
            print("record_list", record_list)
            index += 1
            if not record_list:
                break
            for i in range(0, len(record_list)):        # 遍历每一条记录，生成rowset
                record = record_list[i]
                rowset_buffer = ()
                if s[0] != '':
                    rowset_buffer += ((s[0], '', record[1]),)
                if p[0] != '':
                    rowset_buffer += ((p[0], '', record[2]),)
                if o[0] != '':
                    rowset_buffer += ((o[0], '', record[3]),)
                for qv in qvlist:
                    q = qv[0]
                    v = qv[1]
                    if q[0] != '' or v[0] != '':  # 说明变量是q/v————变量可能没有值
                        if q[0] != '' and v[0] != '':
                            if len(record) == 5:  # 这条记录有spoqv，即变量一定有值
                                tuple_buffer = (q[0],)
                                tuple_id = ()
                                tuple_qv = ()
                                for m in range(len(record[4])):
                                    if not str(record[4][m][0]).startswith('分词'):
                                        one_of_qv_dict = {record[4][m][0]: record[4][m][1]}
                                        one_of_qv = json.dumps(one_of_qv_dict, ensure_ascii=False)
                                        tuple_qv += (one_of_qv,)
                                        tuple_id += ("",)
                                tuple_buffer += (tuple_id,)
                                tuple_buffer += (tuple_qv,)
                                rowset_buffer += (tuple_buffer,)
                                # for jj in range(0, len(variable_list)):
                                #     if variable_list[jj] == 3 or 4:
                                #         variable_list[jj] = -1
                            elif len(record_list[i]) == 4:  # 这条记录只有spo，即变量没有值
                                pass
                        else:
                            if len(record_list[i]) == 5:  # 这条记录有spoqv，即变量一定有值
                                tuple_buffer = ()
                                if q[0] != '':   # 查询q，根据v进行筛选
                                    tuple_buffer += (q[0],)
                                    tuple_id = ()
                                    tuple_q = ()
                                    for m in range(0, len(v[1])):  #遍历所有q
                                        for n in range(0, len(record[4])):
                                            if v[1][m] == f2j(record[4][n][1]):
                                                tuple_id += ("",)
                                                tuple_q += (record[4][n][0],)
                                    tuple_buffer += (tuple_id,)
                                    tuple_buffer += (tuple_q,)
                                elif v[0] != '':  # 查询v，根据q进行筛选
                                    tuple_buffer += (v[0],)
                                    tuple_id = ()
                                    tuple_v = ()
                                    for m in range(0, len(q[1])):  #遍历所有q
                                        for n in range(0, len(record[4])):
                                            if q[1][m] == f2j(record[4][n][0]):
                                                tuple_id += ("",)
                                                tuple_v += (record[4][n][1],)
                                    tuple_buffer += (tuple_id,)
                                    tuple_buffer += (tuple_v,)
                                rowset_buffer += (tuple_buffer,)
                            elif len(record_list[i]) == 4:      # 这条记录只有spo，即变量没有值
                                tuple_buffer = ()
                                tuple_buffer += (("",),) + (("",),)
                                rowset_buffer += (tuple_buffer,)
                if need_factid:
                    rowset_buffer += (('?factID', '', record[0]),)
                if callback != None:
                    callback((index-2)*sizePage+i+1, length, rowset_buffer)
                my_rowset.append(rowset_buffer)
        return my_rowset

    def post(self, id, s, p, o, list_of_qv):
        if id:
            pass
        else:
            id=generate_id()
        session = self.Session()
        spo=SPO(factID=id,s=s,p=p,o=o)
        session.add(spo)
        try:
            session.commit()
        except:
            session.rollback()

        for qv in list_of_qv:
            qv= QV(factID=id,q=qv[0],v=qv[1])
            session.add(qv)
        try:
            session.commit()
        except:
            session.rollback()
        # session.close()


    # 删除指定id的记录，id是字符串类型，如'2000',如果异常，如删除不存在的记录，则输出"删除异常"
    # 注：如果在同一个程序中，先执行delete，再用search查找这个记录，是可以查找到的，但下次再执行程序时就查找不到了，具体原因我也不太清楚
    def delete_by_id(self, id):
        session=self.Session()
        if len(id)>0:
            for i in id:
                session.query(SPO).filter(SPO.factID==i).delete()
                session.query(QV).filter(QV.factID==i).delete()
            session.commit()
            # session.close()
            return 1
        else:
            pass    # 要删除的数据不存在
        return 0

    # 从json文本中导入数据，text是一条json格式的数据，如text=[{"s":"石原里美","p":"职业","o":"艺人","qv":{"作品":"校阅女孩河野悦子","1":"2","3":"4"}}]
    def load_form_text(self,text,id=None):
        try:
            for spoqv in text:
                data = dbutils.pro_fulltext(self, spoqv)
                list_of_qv=[]
                for qv in data['qv']:
                    list_of_qv.append((qv, data['qv'][qv]))  # '.items()[0],qv.items()[1]))
                if id:
                    self.post(id=id, s=data['s'], p=data['p'], o=data['o'], list_of_qv=list_of_qv)
                else:
                    self.post(id=generate_id(), s=data['s'], p=data['p'], o=data['o'], list_of_qv=list_of_qv)
                break;
        except:
            pass    # 插入异常
        return

    # 2021/7/21 未使用  从json文件中导入数据，filename是绝对路径
    def load_form_file(self, filename):
        file = open(filename, 'r', encoding='utf-8-sig')
        if file is None:
            return
        json_content = json.load(file)
        try:
            for text in json_content:
                self.load_from_text(text,id)
        except:
            pass    # 插入异常
        return

    # 查找与phrase相似度在threshhold以上的's'的值，threshhold在0-1之间，phrase为字符串类型
    def get_similar(self, phrase, threshold):
        ans = self.es.search(index=self.db, body={"query": {"match": {"s": {"query": phrase, "fuzziness": 2}}}})
        max_score = ans['hits']['max_score']  # 记下最大相关数值，用来做归一化
        similar_ans = ans['hits']['hits']  # 记录返回的所有相关数据
        ans_list = []
        for i in range(len(similar_ans)):  # 从中找出归一化相关度大于threshold的，并以（值，相关度）元组形式添加到返回的列表里
            corel = similar_ans[i]['_score'] / max_score  # 归一化相关度
            if corel < threshold:  # 已知es查询返回结果按相关度降序排列
                break
            ans_list.append((similar_ans[i]['_source']['s'], corel))
        ans_list = set(ans_list)  # 去重
        ans_list = list(ans_list)
        return ans_list

        # args为json格式

    def search_id(self, args):
        s=args[0]["s"]
        p=args[0]["p"]
        o=args[0]["o"]
        SQL = """SELECT DISTINCT SPO.factID FROM SPO\n"""
        if "qv" in args[0].keys():
            for i in range(len(args[0]['qv'])):
                SQL += "LEFT JOIN QV AS QV" + str(i + 1) + " ON (SPO.factID = QV" + str(i + 1) + ".factID)\n"
        SQL += "WHERE\n"
        if s!="":
            SQL += "SPO.s='" + str(s)+ "' AND\n"
        if p!="":
            SQL += "SPO.p='" + str(p)+ "' AND\n"
        if o!="":
            SQL += "SPO.o='" + str(o)+ "' AND\n"
        if "qv" in args[0].keys():
            for i,qv in enumerate(args[0]['qv'].items()):
                SQL += "QV" + str(i + 1) + ".q='" + str(qv[0]) + "' AND\n"
                SQL += "QV" + str(i + 1) + ".v='" + str(qv[1]) + "' AND\n"
        SQL = SQL[0: -4]
        session=self.Session()
        cursor = session.execute(SQL)
        res = cursor.fetchall()
        result=[]
        for item in res:
            result.append(item[0])
        # session.close()
        return result


if __name__ == '__main__':
    db = Eqldatabase('','','','','test')
    ids=db.search_id([{'s':'萧伯纳','p':'所获奖项','o':'诺贝尔文学奖','qv':{}}])
    # db.delete_by_id(ids)
    # ids = db.search_id([{'s': '萧伯纳', 'p': '所获奖项', 'o': '诺贝尔文学奖', 'qv': {}}])
    print(ids)
    db.post('','萧伯纳','所获奖项','诺贝尔文学奖',[])
    ids = db.search_id([{'s': '萧伯纳', 'p': '所获奖项', 'o': '诺贝尔文学奖', 'qv': {}}])
    print(ids)
    # db.search(('', []), ('', []), ('', []), [])
    # db.get_qv_by_factID('Q392$59ea1983-4db7-7a00-4b74-17fea84baf5e')

    # 测试search2
    # res = db.search2(('?', []), ('?', []), ('?x', []), [])
    # print(len(res))
    # 测试search_simple
    res = db.search_simple(['萧伯纳'], '', '')
    print(res)