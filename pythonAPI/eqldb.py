import json
import time
import requests


class EqlDB:
    def __init__(self, host, port, database, username, password):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.baseurl = 'https://' + self.host + ':' + str(self.port)
        self.token = self.login()

    def login(self):
        if self.username is None or self.username == '':
            token = 'anonymous'
        else:
            try:
                url = self.baseurl + '/token/' + self.username + '/' + self.password
                res = requests.get(url).text
                j = json.loads(res, encoding='utf-8')
                token = j.get('token')
            except (requests.exceptions.ConnectionError, json.JSONDecodeError):
                token = None
        return token

    def logout(self):
        try:
            url = self.baseurl + '/token/' + self.token
            res = requests.delete(url).text
            j = json.loads(res, encoding='utf-8')
            accepted = j.get('accepted')
        except (requests.exceptions.ConnectionError, json.JSONDecodeError):
            accepted = False
        return accepted

    def run(self, eql):
        try:
            url = self.baseurl + '/eql/' + self.database + '/zh/' + str(self.token)
            data = {'q': eql}
            res = requests.post(url, data).text
            j = json.loads(res, encoding='utf-8')
            eql_id = j.get('eql_id')
        except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError, json.JSONDecodeError):
            eql_id = None
        return eql_id

    def stop(self, eql_id):
        try:
            url = self.baseurl + '/eql/' + str(eql_id) + '/' + str(self.token)
            res = requests.delete(url).text
            j = json.loads(res, encoding='utf-8')
            accepted = j.get('accepted')
        except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError, json.JSONDecodeError):
            accepted = False
        return accepted

    def parse(self, eql):
        try:
            url = self.baseurl + '/eql/parser'
            data = {'q': eql}
            res = requests.post(url, data).text
            j = json.loads(res, encoding='utf-8')
            syntax = j.get('syntax')
            if syntax == 'error':
                pos = j.get('token').get('lexpos')
            else:
                pos = 0
        except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError, json.JSONDecodeError):
            syntax = ''
            pos = -1
        return syntax, pos

    def commit(self, eql_id):
        try:
            url = self.baseurl + '/eql/' + str(eql_id) + '/agree/yes/' + str(self.token)
            res = requests.put(url).text
            j = json.loads(res, encoding='utf-8')
            accepted = j.get('accepted')
        except (requests.exceptions.ConnectionError, json.JSONDecodeError):
            accepted = False
        return accepted

    def fetch(self, eql_id, timeout):
        index = 0
        time0 = time.time()
        answer = []
        while True:
            if time.time() - time0 > timeout:
                return 'timeout', answer
            try:
                url = self.baseurl + '/eql/' + str(eql_id) + '/result/' + str(index)
                res = requests.get(url).text
                js = json.loads(res, encoding='utf-8')
                for j in js:
                    status = j.get('s')
                    if status == 'invalid':
                        return 'authenticate error or connection reset', answer
                    if status == 'error':
                        return 'eql syntax error,' + str(j.get('r')), answer
                    if status == 'done':
                        return 'ok', answer
                    if status == 'output':
                        recs = json.loads(j.get('r'))
                        ans = {}
                        for rec in recs:
                            ans[rec.get('var')] = rec.get('label')
                        answer.append(ans)
                index += len(js)
            except (requests.exceptions.ConnectionError, json.JSONDecodeError):
                pass

    def execute(self, eql, timeout=30):
        if str(eql).strip().startswith('\\suggest'):
            syntax, pos = self.parse(eql)
            if syntax == 'error':
                ans = 'syntax error, {"pos":' + str(pos) + '}'
            else:
                eql_id = self.run(eql)
                ans = self.commit(eql_id)
                self.stop(eql_id)
        else:
            eql_id = self.run(eql)
            ans = self.fetch(eql_id, int(timeout))
        return ans

    def close(self):
        return self.logout()