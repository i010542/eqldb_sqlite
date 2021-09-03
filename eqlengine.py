import threading
import time
import dbutils
import os
from random import random

from flask import Flask, request,send_from_directory
import logging
import datetime
import json

# 底层数据库切换
import configparser
database_conf = configparser.ConfigParser()
database_conf.read('conf/database.conf')
underLyingDatabase = database_conf.get('db', 'underLyingDatabase')
if underLyingDatabase == 'elasticsearch':
    from eqldatabase import Eqldatabase
elif underLyingDatabase == 'sqlite':
    from eqldatabase_sqlite import Eqldatabase

import eqllex
import hashlib
from flask_cors import *
import eqlinterp2
import eqlparse
from eqlutils import to_md5
from werkzeug.utils import secure_filename
import xlrd, csv

import requests
import psutil



app = Flask(__name__)
app.config['SEND_STATIC_FILE_MAX_AGE_DEFAULT'] = datetime.timedelta(seconds=1)
handler = None

tokens = {}
CORS(app, supports_credentials=True)     # 设置跨域

super_token = to_md5(str(time.time()))


def verify_token_super(token):
    if tokens.get(token) is not None:
        if tokens.get(token)[1] == super_token:
            return tokens.get(token)[0]
    return ''


def verify_token(token, db, action):
    anonymous_read = False
    active = True
    eql = eqlinterp2.EqlInterpreter('eqldb_db')
    recs = eql.db.search_simple([db], ['instance of'], ['database'])
    if len(recs) == 0:
        anonymous_read = True
    for rec in recs:
        if rec.get('_source').get('qv').get('anonymous') == 'true':
            anonymous_read = True
        if rec.get('_source').get('qv').get('active') == 'false':
            active = False

    user = verify_token_super(token)
    if active and user != '':
        return user

    t = tokens.get(token)
    if t is not None:
        user = t[0]
        if user == 'anonymous' and action == 'write':
            return ''
        dbs = t[1]
        if dbs is None:
            dbs = []
        if db in dbs and active:
            return user
    else:
        user = 'anonymous'

    if action == 'read' and anonymous_read and active:
            return user

    return ''


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

md5 = hashlib.md5()
def make_id(s):
    md5.update(s.encode('utf-8'))
    id = md5.hexdigest()
    return id

@app.route('/', methods=['GET'])
def index_html():
    return app.send_static_file('index.html')


@app.route('/<filename>', methods=['GET'])
def root_file(filename):
    return app.send_static_file(filename)


@app.route('/token/<user>/<pwd>', methods=['GET'])
def get_token(user, pwd):
    res = {"accepted": "false"}
    if request.method == 'GET':
        if user == eqlinterp2.eqlengine_conf.get('eql', 'user') and pwd == eqlinterp2.eqlengine_conf.get('eql', 'pwd'):
            token = make_id(str(time.time()))
            tokens[token] = (user,super_token)
            eql = eqlinterp2.EqlInterpreter('eqldb_db')
            recs = eql.db.search_simple('', ['instance of'], ['database'])
            dbs = []
            for rec in recs:
                if rec.get('_source').get('qv').get('active') != 'false':
                    dbs.append(rec.get('_source').get('s'))
            res = {"accepted": "true", "token": token, "super": "true", "databases": dbs}
        elif user == 'anonymous':
            eql = eqlinterp2.EqlInterpreter('eqldb_db')
            recs = eql.db.search_simple('', ['instance of'], ['database'])
            dbs = []
            for rec in recs:
                if rec.get('_source').get('qv').get('anonymous') == 'true'\
                        and rec.get('_source').get('qv').get('active') != 'false':
                    dbs.append(rec.get('_source').get('s'))
            if len(dbs) > 0:
                token = make_id(str(time.time()))
                tokens[token] = (user, dbs)
                res = {"accepted": "true", "token": token, "super": "false", "databases": dbs}
        else:
            eql = eqlinterp2.EqlInterpreter('eqldb_db')
            recs = eql.db.search_simple('', ['instance of'], ['database'])
            dbs = []
            for rec in recs:
                if rec.get('_source').get('qv').get('username') == user\
                        and rec.get('_source').get('qv').get('password') == to_md5(pwd) \
                        and rec.get('_source').get('qv').get('active') != 'false':
                    dbs.append(rec.get('_source').get('s'))
            if len(dbs) > 0:
                token = make_id(str(time.time()))
                tokens[token] = (user, dbs)
                res = {"accepted": "true", "token": token, "super": "false", "databases": dbs}
    return json.dumps(res, ensure_ascii=False)


@app.route('/token/<token>', methods=['DELETE'])
def delete_token(token):
    if tokens.get(token) is not None:
        tokens.pop(token)
        res = {"accepted": "true"}
    else:
        res = {"accepted": "false"}
    return json.dumps(res, ensure_ascii=False)


@app.route('/db/<db>/<token>', methods=['POST','DELETE'])
def db_post_delete(db,token):
    ret = False
    user = verify_token_super(token)
    if user != '':
        if request.method == 'POST':
            eqlengine_log(request.remote_addr, db, 'post eqldb '+db, '?', '?', user)
            ret = eqlinterp2.eql_db_post_delete(db, 'post')
    user = verify_token(token, db, 'write')
    if user != '':
        if request.method == 'DELETE':
            eqlengine_log(request.remote_addr, db, 'delete eqldb ' + db, '?', '?', user)
            ret = eqlinterp2.eql_db_post_delete(db, 'delete')
    return json.dumps({"accepted": ret}, ensure_ascii=False)

@app.route('/eql/list/<token>', methods=['GET'])
def eql_list(token):
    res = []
    if verify_token_super(token):
        if request.method == 'GET':
            res = eqlinterp2.get_eqls()
    return json.dumps(res,ensure_ascii=False)


@app.route('/eql/lexer', methods=['GET'])
def eql_lexer():
    q = request.values.get('q')
    res = eqllex.lex_parse(q)
    return json.dumps(res, ensure_ascii=False)


@app.route('/eql/parser', methods=['GET','POST'])
def eql_parser():
    q = request.values.get('q')
    syntax, error = eqlparse.syntax_parse(q)
    #print('syntax',syntax)
    if error is None and syntax is not None:
        res = {'syntax': 'correct', 'res': syntax[0][0]}
    else:
        if error is not None:
            res = {'syntax': 'error', 'token': {'type': error.type, 'value': error.value,
                                                'lineno': error.lineno, 'lexpos': error.lexpos}}
        else:
            res = {'syntax': 'error', 'token': {}}
    return json.dumps(res, ensure_ascii=False)


def eqlengine_log(ip, database, q, lang, eql_id, user):
    if database.endswith('_log'):
        logdbname = 'eqldb_log'
    else:
        logdbname = database + '_log'
    db = Eqldatabase(eqlinterp2.eqlengine_conf.get('es', 'host'), eqlinterp2.eqlengine_conf.get('es', 'port'), eqlinterp2.eqlengine_conf.get('es', 'user'),
                     eqlinterp2.eqlengine_conf.get('es', 'pwd'), logdbname)
    da = datetime.datetime.now().strftime("%Y-%m-%d")
    ti = datetime.datetime.now().strftime("%H:%M:%S")
    db.post(str(time.time()) + str(random()), eql_id, 'eql', q, [
        ('ip', ip), ('db', database), ('lang', lang), ('date', da), ('time', ti), ('user', user)])


@app.route('/eql/<db>/<lang>', methods=['POST'])
def eql_run(db, lang):
    user = verify_token('anonymous', db, 'read')
    if user != '':
        if request.method == 'POST':
            ip = request.remote_addr
            q = request.values.get('q')
            print('q', q)
            eql_id = make_id(str(time.time()))
            eqlengine_log(ip, db, q, lang, eql_id, user)
            thread_eql = threading.Thread(target=eqlinterp2.eql_interpret, args=(ip, db, q, lang, eql_id))
            thread_eql.start()
            res = {'eql_id': eql_id}
            return json.dumps(res,ensure_ascii=False)


@app.route('/eql/<db>/<lang>/<token>', methods=['POST'])
def eql_run_token(db, lang, token):
    eql_id = 'None'
    user = verify_token(token, db, 'read')
    if user != '':
        if request.method == 'POST':
            ip = request.remote_addr
            q = request.values.get('q')
            eql_id = make_id(str(time.time()))
            eqlengine_log(ip, db, q, lang, eql_id, user)
            thread_eql = threading.Thread(target=eqlinterp2.eql_interpret, args=(ip, db, q, lang, eql_id))
            thread_eql.start()
    res = {'eql_id': eql_id}
    return json.dumps(res,ensure_ascii=False)

@app.route('/eql/<eql_id>/<token>', methods=['DELETE'])
def eql_stop(eql_id, token):
    res = {"accepted":"false"}
    eql = eqlinterp2.eqls.get(eql_id)
    if eql is not None:
        if verify_token(token, eql.db.db, 'write') != '':
            res = eqlinterp2.delete_eql(eql_id)
    return json.dumps(res, ensure_ascii=False)

@app.route('/eql/<eql_id>/result/<int:index>', methods=['GET'])
def result(eql_id,index):
    res = eqlinterp2.eql_interpret_result(eql_id, int(index))
    return json.dumps(res,ensure_ascii=False)

@app.route('/entity/<db>/<label>/dup', methods=['GET'])
def entity_dup(db,label):
    eql = eqlinterp2.EqlInterpreter(db)
    res = eql.get_duplicate(label)
    return json.dumps(res, ensure_ascii=False)

@app.route('/entity/<db>/<label>/sim', methods=['GET'])
def entity_sim(db,label):
    eql = eqlinterp2.EqlInterpreter(db)
    res = eql.get_similar(label)
    return json.dumps(res, ensure_ascii=False)

@app.route('/entity/<db>/<label>/info', methods=['GET'])
def entity_des(db,label):
    eql = eqlinterp2.EqlInterpreter(db)
    res = eql.get_des(label)
    return json.dumps(res, ensure_ascii=False)

@app.route('/entity/<db>/<label>/id', methods=['GET'])
def entity_id(db,label):
    eql = eqlinterp2.EqlInterpreter(db)
    res = {'id': dbutils.label2wid(eql.db, label)}
    return json.dumps(res, ensure_ascii=False)

@app.route('/entity/<db>/<label>/<token>', methods=['GET', 'PUT', 'POST'])
def entity(db, label, token):
    eql = eqlinterp2.EqlInterpreter(db)
    if request.method == 'GET':
        if verify_token(token, eql.db.db, 'read') != '':
            id = dbutils.label2wid(eql.db, label)
            res = eql.db.search_simple([id], ['_alias', '_freq', '_label'], '')
            return json.dumps(res, ensure_ascii=False, indent=4)
    else:
        if verify_token(token, eql.db.db, 'write') != '':
            data = request.values.get('data')
            if data is not None:
                try:
                    js = json.loads(data, encoding='utf-8')
                    for rec in js:
                        eql.db.load_form_text([rec.get('_source')], rec.get('_id'))
                    return json.dumps({"accepted": "true"}, ensure_ascii=False, indent=4)
                except:
                    return json.dumps({"accepted": "false", "msg": "data format error"}, ensure_ascii=False, indent=4)


@app.route('/eql/<eql_id>/agree/<yn>/<token>', methods=['PUT'])
def audit(eql_id,yn,token):
    res = {"accepted": "false"}
    eql = eqlinterp2.eqls.get(eql_id)
    if eql is not None:
        if verify_token(token, eql.db.db, 'write') != '':
            res = eqlinterp2.eql_interpret_audit(eql_id, yn)
            #eql.db.es.indices.refresh(index=eql.db.db)
    return json.dumps(res, ensure_ascii=False)

@app.route('/debug__', methods=['GET'])
def debug__():
    res = ''
    for eql_id in eqlinterp2.eqls:
        eql = eqlinterp2.eqls.get(eql_id)
        s = 'eql_id: ' + eql_id + ', data: ' + eql.data + ', rowsets: ' + str(len(eql.rowsets)) + ', progress: ' + str(len(eql.progress))
        res += s + '<br>\r\n'
        #for p in eql.progress:
        #    res += '　　' + str(p) + '<br>\r\n'
        if len(eql.progress) > 0:
            res += '　　' + str(eql.progress[len(eql.progress)-1]) + '<br>\r\n'
    res += 'eql total:' + str(len(eqlinterp2.eqls)) + ', threads: ' + str(threading.active_count())
    return res


@app.route('/alan/upload',methods=['POST','GET'])
def alan_upload():
    avata = request.files.get('uploadFile')
    fn = request.form['filename']
    ext = os.path.splitext(fn)[-1]
    folder = '/home/mysql/alan/' + ext[1:]
    if not os.path.exists(folder):
        os.mkdir(folder)
    avata.save(folder + '/' + fn)
    return 'http://lemon.net.cn:8080/alan/' + ext[1:] + '/' + fn


@app.route('/upload/<db>/<token>',methods=['POST','GET'])
def db_upload(db, token):
    user = verify_token(token, db, 'write')
    if user != '':
        avata = request.files.get('uploadFile')
        fn = request.form['filename']
        ext = os.path.splitext(fn)[-1]
        filename = make_id(str(time.time())) + ext
        dt = datetime.datetime.now()
        # basedir =
        folder = eqlinterp2.eqlengine_conf.get('files', 'basedir') + '/' + db + '/' + dt.strftime('%Y%m%d') + '/' + dt.strftime('%H%M')
        if not os.path.exists(folder):
            os.makedirs(folder)
        avata.save(folder + '/' + filename)
        eqlengine_log(request.remote_addr, db, 'upload file:'+folder + '/' + filename, '', '', user)
        return eqlinterp2.eqlengine_conf.get('files', 'baseurl') + '/' + folder + '/' + filename
    else:
        return 'verify token error'


# -------------------------------------- apis added by ZHF, 2021-07-02 -------------------------------------#
# 1. 修改密码
@app.route('/change-password/<db>/<oldPassword>/<newPassword>/<token>', methods=['POST'])
def change_password(db, oldPassword, newPassword, token):
    user = verify_token(token, db, 'read')
    eql = eqlinterp2.EqlInterpreter('eqldb_db')
    rec = eql.db.search_simple([db], ['instance of'], ['database'])[0]

    id = rec.get('_id')
    qv = rec.get('_source').get('qv')

    inviter = qv.get('inviter')
    name = qv.get('name')
    email = qv.get('email')
    phone = qv.get('phone')
    username = qv.get('username')
    password = qv.get('password')
    anonymous = qv.get('anonymous')
    tm = qv.get('time')

    if to_md5(oldPassword) != password:
        res = {"accepted": "false", "reason": "old password not correct"}
    elif user != username:
        res = {"accepted": "false", "reason": "limits of authority"}
    else:
        eql.db.delete_by_id([id])
        eql.db.post(str(time.time()) + str(random()), db, 'instance of', 'database', [
            ('inviter', inviter), ('name', name), ('email', email), ('phone', phone), ('username', username),
            ('password', to_md5(newPassword)), ('anonymous', anonymous), ('time', tm)])
        res = {"accepted": "true"}

    return json.dumps(res)

# 2. 导入
# 3. 导出

# 5. 普通用户的数据库管理
@app.route('/dbInfo/<dbName>/<token>')
def dbInfo(dbName, token):
    user = verify_token(token, dbName, 'read')
    eql = eqlinterp2.EqlInterpreter('eqldb_db')
    rec = eql.db.search_simple([dbName], ['instance of'], ['database'])[0]

    qv = rec.get('_source').get('qv')

    username = qv.get('username')
    if user != username and verify_token_super(token) == "":
        res = {"accepted": "false", "reason": "limits of authority"}
    else:
        inviter = qv.get('inviter')
        name = qv.get('name')
        email = qv.get('email')
        phone = qv.get('phone')
        active = qv.get('active')
        if active == None:
            active = ""
        anonymous = qv.get('anonymous')
        tm = qv.get('time')

        if underLyingDatabase == 'elasticsearch':
            recordCount = eval(requests.get("http://elastic:lemon888@127.0.0.1:9200/_cat/indices?index=" + dbName + "&format=json").content)[0].get('docs.count')
            if dbName[-4:] == "_log":
                logCount = "-"
            else:
                logCount = eval(requests.get("http://elastic:lemon888@127.0.0.1:9200/_cat/indices?index=" + dbName + "_log&format=json").content)[0].get('docs.count')
        elif underLyingDatabase == 'sqlite':
            recordCount = ''
            logCount = ''
            # todo !!!
        res = {"accepted": "true", "recordCount": recordCount, "logCount": logCount, "inviter": inviter, "name": name,
               "username": username, "email": email, "phone": phone, "createTime": tm, "lastActiveTime": "",
               "anonymous": anonymous, "active": active}

    return json.dumps(res)


# 5.1 anonymous/status的修改
@app.route('/change/<field>/<to>/<dbName>/<token>', methods=['POST'])
def change_field(field, to, dbName, token):
    user = verify_token(token, dbName, 'read')
    eql = eqlinterp2.EqlInterpreter('eqldb_db')
    rec = eql.db.search_simple([dbName], ['instance of'], ['database'])[0]

    id = rec.get('_id')
    qv = rec.get('_source').get('qv')

    username = qv.get('username')

    if field == 'anonymous':
        if qv['anonymous'] != None:
            qv['anonymous'] = to
    elif field == 'status':
        if qv['active'] != None:
            qv['active'] = to

    qvList = []
    for key in qv.keys():
        qvList.append((key, qv.get(key)))

    if user != username and verify_token_super(token) == "":
        res = {"accepted": "false", "reason": "limits of authority"}
    else:
        eql.db.delete_by_id([id])
        eql.db.post(str(time.time()) + str(random()), dbName, 'instance of', 'database', qvList)
        res = {"accepted": "true"}

    return json.dumps(res)


# 6. 管理员的数据库管理
@app.route('/database-manage/<token>', methods=['GET'])
def database_manage(token):
    if verify_token_super(token):
        eql = eqlinterp2.EqlInterpreter('eqldb_db')
        recs = eql.db.search_simple('', ['instance of'], ['database'])
        dbInfos = []
        for rec in recs:
            dbName = rec.get('_source').get('s')
            if dbName[-4:] == "_log":
                pass
            else:
                if underLyingDatabase == 'elasticsearch':
                    recordCount = eval(requests.get("http://elastic:lemon888@127.0.0.1:9200/_cat/indices?index=" + dbName + "&format=json").content)[0].get('docs.count')
                    status = rec.get('_source').get('qv').get('active')
                    if status == None:
                        status = ''
                    # status = eval(requests.get("http://elastic:lemon888@127.0.0.1:9200/_cat/indices?index=" + dbName + "&format=json").content)[0].get('status')
                    log = eval(requests.get("http://elastic:lemon888@127.0.0.1:9200/_cat/indices?index=" + dbName + "_log&format=json").content)

                    dbInfo = {"name": dbName, "status": status, "recordCount": recordCount}
                    if isinstance(log, list):
                        dbInfo["logCount"] = log[0].get("docs.count")
                    else:
                        dbInfo["logCount"] = 0
                elif underLyingDatabase == 'sqlite':
                    dbInfo = {"name": dbName, "status": '', "recordCount": '', 'logCount': ''}
                    # todo !!!
                dbInfos.append(dbInfo)
        res = {"accepted": "true", "dbInfos": dbInfos}
        return json.dumps(res)
    else:
        res = {"accepted": "false", "reason": "limits of authority"}
    return json.dumps(res)


@app.route('/availability/test', methods=['GET'])
def availability_test():
    res = {'accepted': 'true'}
    return json.dumps(res)


# 7. 服务器信息
@app.route('/server-info/<token>', methods=['GET'])
def server_info(token):
    if verify_token_super(token):
        cpu_usage_rate = '%.2f%%' % (psutil.cpu_percent(1))  # 1s内cpu使用率
        mem = psutil.virtual_memory()
        mem_usage_rate = '%.2f%%' % (mem[2])
        mem_total = str(int(mem[0] / 1024 / 1024)) + 'M'
        mem_used = str(int(mem[3] / 1024 / 1024)) + 'M'
        net1 = psutil.net_io_counters()  # 云服务器网速
        net_sent1 = float(net1[0]) / 1024
        net_receive1 = float(net1[1])/ 1024
        time.sleep(0.001)
        net2 = psutil.net_io_counters()  # 1s后云服务器网速
        net_sent2 = float(net2[0]) / 1024
        net_receive2 = float(net2[1]) / 1024
        send_speed = net_sent2 - net_sent1
        receive_speed = net_receive2 - net_receive1

        send_unit = 'K/s'
        receive_unit = 'K/s'
        send_speed *= 1000
        receive_speed *= 1000
        # if send_speed > 1024:
        send_speed = send_speed / 1024
        send_unit = 'M/s'
        # if receive_speed > 1024:
        receive_speed = receive_speed / 1024
        receive_unit = 'M/s'
        send = format(send_speed, '.2f')
        receive = format(receive_speed, '.2f')
        res = {"accepted": "true", 'cpu_usage_rate': cpu_usage_rate.split('%')[0],
               'mem_usage_rate': mem_usage_rate.split('%')[0], 'mem_total': mem_total, 'mem_used': mem_used,
               'send_speed': send, 'receive_speed': receive}
    else:
        res = {"accepted": "false", "reason": "limits of authority"}
    return json.dumps(res)


# 8. 连接管理
@app.route('/connection-manage/<token>', methods=['GET'])
def connection_manage(token):
    if verify_token_super(token):
        def format_secs(seconds):
            seconds = int(seconds) / 1000
            m, s = divmod(seconds, 60)
            h, m = divmod(m, 60)
            d, h = divmod(h, 24)
            if d == 0:
                if h == 0:
                    formated = "{}分{}秒".format(int(m), int(s))
                else:
                    formated = "{}小时{}分{}秒".format(int(h), int(m), int(s))
            else:
                formated = "{}天{}小时{}分{}秒".format(int(d), int(h), int(m), int(s))
            return formated
        eqls = eqlinterp2.get_eqls()
        eqls.sort(key=lambda x: x.get('duration'), reverse=True)
        for eql in eqls:
            eql['duration'] = format_secs(eql.get('duration'))
        res = {"accepted": "true", "connections": eqls}
    else:
        res = {"accepted": "false", "reason": "limits of authority"}
    return json.dumps(res)


# -------------------------------------- apis added by HB, 2021-07-02 -------------------------------------#
# 2. 导入
@app.route('/upload-file/<db>/<token>', methods=['POST', 'OPTIONS'])
def upload_file(db, token):
    user = verify_token(token, db, 'read')
    if user!='':
        eqlengine = eqlinterp2.EqlInterpreter(db)
        basepath = os.path.abspath(os.path.join(__file__,'../'))
        time_now = time.time().__str__()

        file = request.files.get('file')
        print('file', file)
        file_name = file.filename
        if file_name[-5:] == '.json':
            upload_path = os.path.join(basepath, 'uploaded_files', secure_filename(
                file.filename + time_now + '.json'))
        elif file_name[-4:] == '.xls':
            upload_path = os.path.join(basepath, 'uploaded_files', secure_filename(
                file.filename + time_now + '.xlsx'))
        elif file_name[-4:] == '.csv':
            upload_path = os.path.join(basepath, 'uploaded_files', secure_filename(
                file.filename + time_now + '.csv'))
        else:
            upload_path = None
            res = {'accepted': 'fail', 'reason': 'extension'}
            return json.dumps(res)
        if upload_path != None:
            file.save(upload_path)
            # try:
            if file_name[-5:] == '.json':
                f = open(upload_path, 'r',encoding='utf-8')
                id=''
                for line in f:
                    if line.startswith(u'\ufeff'):
                        line = line.encode('utf8')[3:].decode('utf8')
                    print('line', line)
                    line_json_content = json.loads(line)
                    print("line_json_content", line_json_content)
                    is_index = line_json_content.get('index')
                    print('is_index', type(is_index))
                    if is_index != None:
                        id=is_index['_id']
                    else:
                        s = line_json_content.get('s')
                        p = line_json_content.get('p')
                        o = line_json_content.get('o')
                        print('spo', s, p, o)
                        qv = line_json_content.get('qv')
                        print(qv)
                        qv= list(qv.items())
                        print(id)
                        if id!='':
                            eqlengine.db.post(id, s, p, o, qv)
                        else:
                            eqlengine.db.post(str(time.time()) + str(random()), s, p, o, qv)
                file.close()
            elif file_name[-4:] == '.xls':
                excel_index = upload_path
                data = xlrd.open_workbook(excel_index, on_demand=True)
                table = data.sheets()[0]
                row_number = table.nrows
                for i in range(0, row_number):
                    number = 0
                    for mystr in table.row_values(i):
                        if mystr != "":
                            number += 1
                    qv=[]
                    if number != 3:
                        qv_num = int((number - 3) / 2)
                        for j in range(0, qv_num - 1):
                            qv.append((str(table.row_values(i)[3 + j * 2]),str(table.row_values(i)[3 + j * 2 + 1])))
                        qv.append((str(table.row_values(i)[3 + (qv_num - 1) * 2]),str(table.row_values(i)[3 + (qv_num - 1) * 2 + 1])))
                    print(qv)
                    eqlengine.db.post(str(time.time()) + str(random()), str(table.row_values(i)[0]),str(table.row_values(i)[1]), str(table.row_values(i)[2]), qv)
                data.release_resources()
            elif file_name[-4:] == '.csv':
                reader = csv.reader(open(upload_path, 'r', encoding='gb18030'))
                for row in reader:
                    number = 0
                    for mystr in row:
                        if mystr != "":
                            number += 1
                    qv=[]
                    if number != 3:
                        qv_num = int((number - 3) / 2)
                        for j in range(0, qv_num - 1):
                            qv.append((str(row[3 + j * 2]),str(row[3 + j * 2 + 1])))
                        qv.append((str(row[3 + (qv_num - 1) * 2]),str(row[3 + (qv_num - 1) * 2 + 1])))
                    print(qv)
                    eqlengine.db.post(str(time.time()) + str(random()),str(row[0]),str(row[1]),str(row[2]),qv)
            res = {'accepted': 'success'}
            # except:
            #     res = {'accepted': 'fail', 'reason': 'upload_fail'}
    else:
        res = {'accepted': 'fail', 'reason': 'token_illegal'}
    return json.dumps(res)

# 3. 导出
@app.route('/get-db-data/<db>/<token>', methods=['GET', 'POST', 'OPTIONS'])
def get_all_data_from_db(db, token):
    user=verify_token(token,db,'read')
    if user!='':
        eqlengine = eqlinterp2.EqlInterpreter(db)
        rec = eqlengine.db.search_simple('', '', '', needAll=True)
        print(rec)
        res = []
        for i in rec:
            res.append({"index": {"_id":i['_id']}})
            res.append(i['_source'])
        file_name = db + '.json'
        path = os.path.abspath(os.path.join(__file__, "../to_download_files")) + '/' + file_name
        f = open(path, 'w', encoding='utf-8')
        for res_ele in res:
            f.write(json.dumps(res_ele, ensure_ascii=False))
            f.write('\n')
        # f.write(json.dumps(res, ensure_ascii=False))
        f.close()   # 注意！！！！！！写完文件要close 否则数据在缓冲区，无法写入文件
        print(file_name)
        return send_from_directory('to_download_files', filename=file_name, as_attachment=True)
    else:
        return json.dumps({'accepted': 'fail'})


def compare(a,b):
    if a['_source']['qv']['time']<b['_source']['qv']['time']:
        return True
    else:
        return False


def takeKey(elem):
    return elem['_source']['qv']['time']


# 4. 系统日志
@app.route('/get-log/<db>/<date>/<token>', methods=['GET'])
def get_log(db, date, token):
    log_db = db + '_log'
    user = verify_token(token, log_db, 'read')
    print(user)
    if user == '':
        res = {"accepted": "false", "reason": "limits of authority"}
    else:
        if db.endswith('_log'):
            res = {"accepted": "false", "reason": "log"}
        else:
            try:
                eqlengine = eqlinterp2.EqlInterpreter(db + '_log')
                rec = eqlengine.db.search2(('?x1', []), ('', ['eql']), ('?x2', []), [(('', ['date']), ('', [date])), (('?x3', []), ('?x3', []))])
                logs = []
                for r in rec:
                    qv = r[2][2]
                    logs.append({
                        'eql': r[1][2],
                        'ip': eval(qv[0]).get("ip"),
                        'db': eval(qv[1]).get("db"),
                        'lang': eval(qv[2]).get("lang"),
                        'time': eval(qv[4]).get("time"),
                        'datetime': eval(qv[3]).get("date") + " " + eval(qv[4]).get("time"),
                        'user': eval(qv[5]).get("user")
                    })
                logs.sort(key=lambda x: x.get('time'), reverse=True)
                res = {"accepted": "true", "logs": logs}
            except:
                res = {"accepted": "false"}
    return json.dumps(res)



@app.route('/test', methods=['GET'])
def test():
    return "api is online"

def cache_8e_labels():
    print('cache_8e_labels start')
    eql = eqlinterp2.EqlInterpreter('davinci_8e')
    res = eql.db.search_simple('', ['_label'], '', True, '_lang', 'zh')
    print('cache_8e_labels end')

if __name__ == '__main__':
    if underLyingDatabase == 'sqlite':
        from importDataToSqlite import init
        init()
    #if underLyingDatabase == 'elasticsearch':
    #    cache_8e_labels()
    if app.logger.handlers:
        app.logger.handlers.pop()
    pem_file = 'lemon.net.cn.pem'
    key_file = 'lemon.net.cn.key'
    if os.path.exists(pem_file) and os.path.exists(key_file):
        app.run(host='0.0.0.0', debug=False, port=8086, ssl_context=(pem_file, key_file))
    else:
        app.run(host='0.0.0.0', debug=True, port=8086)
    # app.run(host='0.0.0.0', debug=True, port=8086)
