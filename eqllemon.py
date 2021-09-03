import dbutils
import threading
import time
import os
from random import random

from flask import Flask, request
import logging
import datetime
import json

import han
import eqlinterp2

# 底层数据库切换
import configparser
database_conf = configparser.ConfigParser()
database_conf.read('conf/database.conf')
underLyingDatabase = database_conf.get('db', 'underLyingDatabase')
if underLyingDatabase == 'elasticsearch':
    from eqldatabase import Eqldatabase
elif underLyingDatabase == 'sqlite':
    from eqldatabase_sqlite import Eqldatabase

from eqlutils import utils, to_md5
from flask_cors import *

app = Flask(__name__)
app.config['SEND_STATIC_FILE_MAX_AGE_DEFAULT'] = datetime.timedelta(seconds=1)
handler = None

tokens = []
CORS(app, supports_credentials=True)     # 设置跨域

def verify_token(token):
    return token in tokens


#@app.before_request
def before_request():
    if app.logger.handlers:
        app.logger.handlers.pop()
    now_date = datetime.datetime.now().strftime('%Y%m%d')
    global handler
    handler = logging.FileHandler('./log/flask' + now_date + '.log')
    app.logger.propagate = False
    app.logger.setLevel(logging.DEBUG)
    app.logger.addHandler(handler)


@app.route('/', methods=['GET'])
def index_html():
    return app.send_static_file('product.html')


@app.route('/<filename>', methods=['GET'])
def root_file(filename):
    return app.send_static_file(filename)

parent_dir = os.path.dirname(os.path.abspath(__file__))

lemon_conf = configparser.ConfigParser()
lemon_conf.read(parent_dir+'/conf/lemon.conf')
lemon_db = Eqldatabase(lemon_conf.get('es', 'host'), lemon_conf.get('es', 'port'), lemon_conf.get('es','user'), lemon_conf.get('es','pwd'), lemon_conf.get('es', 'index'))


@app.route('/lemon/simple/<s>/<p>', methods=['GET'])
def lemon_simple_sp(s, p):
    lang = han.get_lang(s + p)
    recs = lemon_db.search_simple([dbutils.label2wid(lemon_db, s)], [dbutils.label2wid(lemon_db, p)], '')
    answer = []
    for rec in recs:
        match = True
        if p == '_label':
            match = lang == rec['_source']['qv']['_lang']
        if match:
            o = dbutils.widx2label(lemon_db, rec['_source']['o'], lang)
            answer.append(o)
    if len(answer) == 1:
        ans = {'answer': answer[0]}
    else:
        ans = {'answer': answer}
    return json.dumps(ans, ensure_ascii=False)


@app.route('/lemon/simple/<s>', methods=['GET'])
def lemon_simple_s(s):
    lang = han.get_lang(s)
    recs = lemon_db.search_simple([dbutils.label2wid(lemon_db, s)], '', '')
    answer = {}
    for rec in recs:
        match = True
        p = dbutils.widx2label(lemon_db, rec['_source']['p'], lang)
        if rec['_source']['p'] == '_label' or rec['_source']['p'] == '_alias':
            match = lang == rec['_source']['qv']['_lang']
        if match:
            o = dbutils.widx2label(lemon_db, rec['_source']['o'], lang)
            if answer.get(p) is None:
                answer[p] = o
            else:
                if isinstance(answer.get(p), list):
                    answer.get(p).append(o)
                else:
                    answer[p] = [answer.get(p), o]
    ans = {'answer': answer}
    return json.dumps(ans, ensure_ascii=False)


@app.route('/lemon/<s>/<p>', methods=['GET'])
def lemon_s_p(s, p):
    if p == '近似':
        sims = utils().get_similar(lemon_db, s, 0)
        if isinstance(sims,list):
            sims = sorted(sims,key=lambda x: x[1], reverse=True)
        else:
            sims = []
        return json.dumps({'answer': sims}, ensure_ascii=False, indent=4)
    if p == '重名':
        dups = dbutils.label2dup(lemon_db, s)
        if not isinstance(dups, list):
            dups = []
        return json.dumps({'answer': dups}, ensure_ascii=False, indent=4)
    if p == '名称':
        return lemon_simple_sp(s, '_label')
    if p == 'ID':
        res = dbutils.label2wid(lemon_db, s)
        return json.dumps({'answer':res})
    if p == '描述':
        res = dbutils.label2des(lemon_db, s)
        return json.dumps({'answer':res}, ensure_ascii=False,indent=4)
    lang = han.get_lang(s + p)
    recs = lemon_db.search_simple([dbutils.label2wid(lemon_db, s)], [dbutils.label2wid(lemon_db, p)], '')
    answer = []
    detail = []
    for rec in recs:
        o = dbutils.widx2label(lemon_db, rec['_source']['o'], lang)
        answer.append(o)
        detail_one = {}
        detail_one[p] = o
        qvs = rec['_source']['qv']
        for q in qvs:
            q2 = dbutils.widx2label(lemon_db, q,lang)
            v2 = dbutils.widx2label(lemon_db, qvs[q],lang)
            detail_one[q2] = v2
        detail.append(detail_one)
    res = {'answer':answer,'detail':detail}
    return json.dumps(res, ensure_ascii=False,indent=4)

# def lemon_s_p(s, p):
#     print(s, p)
#     if p == '重名':
#         dups = lemon_db.label2dup(s)
#         if not isinstance(dups, list):
#             dups = []
#         return json.dumps({'answer': dups}, ensure_ascii=False, indent=4)
#     if p == '名称':
#         return lemon_simple_sp(s, '_label')
#     if p == 'ID':
#         res = lemon_db.label2wid(s)
#         return json.dumps({'answer':res})
#     if p == '描述':
#         res = lemon_db.label2des(s)
#         return json.dumps({'answer':res}, ensure_ascii=False,indent=4)
#     ip = request.remote_addr
#     q = s + ':' + p + ':' + '?y(?z)'  # \\group by ?x'
#     eql_id = time.time().__str__()
#     thread_eql = threading.Thread(target=eqlinterp2.eql_interpret, args=(ip, lemon_db, q, 'zh', eql_id))
#     thread_eql.start()
#     index = 0
#     answer = []
#     detail = []
#     recs = []
#     time0 = time.time()
#     while True:
#         if time.time() - time0 > 300:
#             recs = ['timeout']
#             break
#         status = ''
#         res = eqlinterp2.eql_interpret_result(eql_id, index)
#         for r in res:
#             status = r.get('s')
#             if status == 'done':
#                 break
#             elif status == 'output':
#                 js = json.loads(r.get('r'))
#                 answer.append(js[0]['label'])
#                 detail_one = {}
#                 detail_one[p] = js[0]['label']
#                 for qv in js[1]['label']:
#                     js1 = json.loads(qv)
#                     item1 = list(js1.items())
#                     detail_one[item1[0][0]] = item1[0][1]
#                 detail.append(detail_one)
#         if status == 'done':
#             break
#         index += len(res)
#     eqlinterp2.delete_eql(eql_id)
#     res = {'answer':answer,'detail':detail}
#     return json.dumps(res, ensure_ascii=False,indent=4)


@app.route('/lemon/<s>', methods=['GET'])
def lemon_s(s):
    lang = han.get_lang(s)
    wid = dbutils.label2wid(lemon_db, s)
    recs = lemon_db.search_simple([wid], '', '')
    ans = {'name': s}
    i = 0
    p_done = []
    while i < len(recs):
        rec = recs[i]
        p = dbutils.widx2label(lemon_db, rec['_source']['p'], lang)
        if not p in p_done:
            ps = []
            for r in recs:
                if dbutils.widx2label(lemon_db, r['_source']['p'], lang) == p:
                    qvs = r['_source']['qv']
                    if len(qvs) == 0:
                        ps.append(dbutils.widx2label(lemon_db, r['_source']['o'], lang))
                    else:
                        qv2 = {}
                        qv2[p] = dbutils.widx2label(lemon_db, r['_source']['o'], lang)
                        for q in qvs:
                            q2 = dbutils.widx2label(lemon_db, q,lang)
                            v2 = dbutils.widx2label(lemon_db, qvs[q],lang)
                            qv2[q2] = v2
                        ps.append(qv2)
            if len(ps) == 1:
                ans[p] = ps[0]
            else:
                ans[p] = ps
            p_done.append(p)
        i += 1
    return json.dumps(ans, ensure_ascii=False, indent=4)

#
# @app.route('/lemon/cloud/<inviter>/<name>/<email>/<phone>/<database>/<username>/<password>/', methods=['GET'])
# def lemon_cloud1(inviter, name, email, phone, database, username, password):
#     if eqlinterp2.eql_db_post_delete(database, 'post'):
#         db = Eqldatabase(lemon_conf.get('es', 'host'), lemon_conf.get('es', 'port'), lemon_conf.get('es', 'user'),
#                          lemon_conf.get('es', 'pwd'), 'eqldb_db')
#         dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#         db.post(str(time.time()) + str(random()), database, 'instance of', 'database', [
#             ('inviter', inviter), ('name', name), ('email', email), ('phone', phone), ('username', username),
#             ('password', to_md5(password)), ('anonymous', 'false'), ('time', dt)])
#         ans = 'true'
#     else:
#         ans = 'false'
#     return json.dumps({'accepted': ans}, ensure_ascii=False)


@app.route('/lemon/cloud', methods=['POST'])
def lemon_cloud():
    inviter = request.values.get('inviter')
    name = request.values.get('name')
    email = request.values.get('email')
    phone = request.values.get('phone')
    database = request.values.get('database')
    username = request.values.get('username')
    password = request.values.get('password')
    if eqlinterp2.eql_db_post_delete(database, 'post') and eqlinterp2.eql_db_post_delete(database+'_log', 'post'):
        db = Eqldatabase(lemon_conf.get('es', 'host'), lemon_conf.get('es', 'port'), lemon_conf.get('es', 'user'),
                         lemon_conf.get('es', 'pwd'), 'eqldb_db')
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        db.post(str(time.time()) + str(random()), database, 'instance of', 'database', [
            ('inviter', inviter), ('name', name), ('email', email), ('phone', phone), ('username', username),
            ('password', to_md5(password)), ('active', 'true'), ('anonymous', 'false'), ('time', dt)])
        db.post(str(time.time()) + str(random()), database+'_log', 'instance of', 'database', [
            ('inviter', inviter), ('name', name), ('email', email), ('phone', phone), ('username', username),
            ('password', to_md5(password)), ('active', 'true'), ('anonymous', 'false'), ('time', dt)])
        ans = 'true'
    else:
        ans = 'false'
    return json.dumps({'accepted': ans}, ensure_ascii=False)

# def lemon_s(s):
#     ip = request.remote_addr
#     q = s + ':?x:' + '?y(?z)'  # \\group by ?x'
#     eql_id = time.time().__str__()
#     thread_eql = threading.Thread(target=eqlinterp2.eql_interpret, args=(ip, lemon_db, q, 'zh', eql_id))
#     thread_eql.start()
#     index = 0
#     recs = []
#     time0 = time.time()
#     while True:
#         if time.time() - time0 > 300:
#             recs = ['timeout']
#             break
#         status = ''
#         res = eqlinterp2.eql_interpret_result(eql_id, index)
#         for r in res:
#             #print(r)
#             status = r.get('s')
#             if status == 'done':
#                 break
#             elif status == 'output':
#                 js = json.loads(r.get('r'))
#                 recs.append(js)
#                 #for j in js:
#                 #    recs.append(j)  #todo .get('label'))
#         if status == 'done':
#             break
#         index += len(res)
#     eqlinterp2.delete_eql(eql_id)
#     ans = {'name':s}
#     i = 0
#     p_done = []
#     while i < len(recs):
#         rec = recs[i]
#         p = rec[0]['label']
#         if not p in p_done:
#             ps = []
#             for r in recs:
#                 if r[0]['label'] == p:
#                     qvs = r[2]['label']
#                     if len(qvs) == 0:
#                         ps.append(r[1]['label'])
#                     else:
#                         qv2 = {}
#                         qv2[p] = r[1]['label']
#                         for qv in qvs:
#                             js = json.loads(qv)
#                             kv = list(js.items())
#                             qv2[kv[0][0]] = kv[0][1]
#                         ps.append(qv2)
#             if len(ps) == 1:
#                 ans[p] = ps[0]
#             else:
#                 ans[p] = ps
#             p_done.append(p)
#         i += 1
#     return json.dumps(ans, ensure_ascii=False,indent=4)


if __name__ == '__main__':
    if app.logger.handlers:
        app.logger.handlers.pop()
    pem_file = 'lemon.net.cn.pem'
    key_file = 'lemon.net.cn.key'
    # if os.path.exists(pem_file) and os.path.exists(key_file):
    #     app.run(host='0.0.0.0', debug=False, port=443, ssl_context=(pem_file, key_file))
    # else:
    #     app.run(host='0.0.0.0', debug=True, port=443)
    app.run(host='0.0.0.0', debug=True, port=8087)


