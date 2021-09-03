import hashlib
import re
import datetime
import dbutils

def to_md5(s):
    md5 = hashlib.md5()
    md5.update(s.encode(encoding='utf-8'))
    id = md5.hexdigest()
    return id

class utils:
    def __init__(self):
        return

    # 对于已知一个x，想找到x.x1.x2..的情况，调用此函数。init_s = 'x', prop_expr = '?x.x1.x2..'
    def get_o(self, eqldata, init_s, prop_expr, option = 0):
        if '.' in prop_expr:
            number = len(prop_expr.split('.'))
            s1 = ('', [init_s])
            p1 = ('', [prop_expr.split('.')[1]])
            firstStep = eqldata.search2(s1, p1, ('?',[]))
            if firstStep == []:
                return ""
            else:
                buffer1 = ()
                if number == 2:  # 1 dot
                    buffer1 = firstStep
                elif number > 2:  # more than 1 dot
                    for m in range(number - 2):
                        ss = ('', [firstStep[0][0][2]])
                        pp = ('', [prop_expr.split('.')[m + 2]])
                        firstStep = eqldata.search2(ss, pp, ('?', []))
                    buffer1 = firstStep
                if buffer1 == []:
                    return ""
                else:
                    if option == 0:     #返回最后一个值
                        return buffer1[-1][0][2]
                    elif option == 1:
                        my_list = []
                        for i in range(len(buffer1)):
                            my_list.append(buffer1[i][0][2])
                        return my_list
        else:
            return init_s

    # 将ans由一个字符串分解为['?x','?y.国籍']的形式
    def resolve(ans):
        ans_list=[]
        str=''
        for c in ans:
            if c==',':
                ans_list.append(str)
                str=''
            else:
                str+=c
        ans_list.append(str)
        return ans_list

    #输入参数rowset，为rowset格式的列表；ans为'?x,?x.母语，?z'格式的字符串 ；返回答案字符串列表
    def print_to_rowset(self,eqldata,rowset,ans):
        ans_list=ans.split(',')
        for k in range(len(rowset)):#对每条记录
            tuple_buffer = ()
            for new_elem in ans_list:#对答案的每个项?x.国籍
                flag=0
                for tup1 in rowset[k]:#对记录的每个元组
                    if new_elem == tup1[0]:  # 将所需要的ans项加到buffer里
                        tuple_buffer+=(tup1,)
                        flag = 1
                if flag:
                    continue
                if '.' in new_elem:#如果所需要的ans项没在里面且是？x.国籍形式的
                    for tup2 in rowset[k]:
                        if new_elem.split('.')[0]==tup2[0]:
                            if isinstance(tup2[2],tuple):
                                for each_value in tup2[2]:
                                    result = self.get_o(eqldata, each_value, dbutils.propexpr2wid(eqldata, new_elem), 1)
                                    id_tup=()
                                    i=0
                                    if len(result):
                                        while i<len(result):
                                            id_tup+=('',)
                                            i=i+1
                                        tuple_buffer += (((new_elem,) + id_tup + tuple(result)),)
                                    else:
                                        tuple_buffer+=(((new_elem,'','')),)
                            else:
                                result=self.get_o(eqldata,tup2[2],dbutils.propexpr2wid(eqldata, new_elem),1)
                                id_tup = ()
                                i = 0
                                if len(result):
                                    while i < len(result):
                                        id_tup += ('',)
                                        i = i + 1
                                    tuple_buffer += (((new_elem,) + id_tup + tuple(result)),)
                                else:
                                    tuple_buffer += (((new_elem, '', '')),)
            rowset[k]=tuple_buffer
        return rowset

    #输入参数value 为需要匹配的值如'美国'， template为匹配模板如'%国'
    def match(self,value,template):#返回结果为False说明不匹配，为True匹配
        i=j=0
        while (i<len(value))and(j<len(template)):
            #如果是\%或者\_结构，首先将下划线去掉，并直接进行判断是否匹配，如果匹配，跳过接下来继续按常规判断，如果不匹配，退出。如果非此种结构，按常规继续判断
            if (template[j]=='\\'):
                if j+1<len(template) and (template[j+1] == '%'or template[j+1] == '_' ):
                    if(template[j+1]==value[i]):
                        i+=1
                        j+=2
                    else:
                        break
            if (i<len(value))and(j<len(template)):
                #如果当前符号是%，且前面已判断非\%结构，则让template跳到下一个符号
                if (template[j]=='%'):
                    j+=1
                #如果当前符号非%，判断两个对应符号是否相同或者有一方是_,如果是，双双加一
                elif template[j]==value[i] or template[j]=='_':
                    i+=1
                    j+=1
                #如果template当前符号的前一个是%,那么：如果相同了，就双双加一，否则只有i+1
                elif j>0 and template[j-1]=='%':
                    if template[j]==value[i]:
                        i+=1
                        j+=1
                    else:
                        i+=1
                #否则，退出
                else:
                    break
            else:
                break
        #r如果最后都到达了末尾，说明匹配
        if i == len(value) and j ==len(template):
            return True
        #如果j到达了末尾，且末尾是%，说明匹配
        elif j==len(template) and template[j-1]=='%':
            return True
        #如果value到达了末尾,且template到达了最后一位，且最后一位是%，说明匹配
        elif i==len(value) and j==len(template)-1 and template[j]=='%':
            return True
        #否则，不匹配
        else:
            return False

    ##输入参数eqldata为eqldatabase的一个实例 ；phrase为需要查询相似数据的短语，如 "萧伯纳",字符串类型 ；threhold为0-1之间的相似度 ; 返回由（相似数据，相似度）元组组成的列表
    def get_similar(self,eqldata,phrase, threshold):
        ans = eqldata.es.search(index=eqldata.db,body={"query": {"match": {"o": {"query": phrase, "fuzziness": 2}}}}, size=1000)#
        max_score = ans['hits']['max_score']  # 记下最大相关数值，用来做归一化
        similar_ans = ans['hits']['hits']  # 记录返回的所有相关数据
        ans_list = []
        if max_score!=0:
            for i in range(len(similar_ans)):  # 从中找出归一化相关度大于threshold的，并以（值，相关度）元组形式添加到返回的列表里
                corel = similar_ans[i]['_score'] / max_score  # 归一化相关度
                if corel < threshold:  # 已知es查询返回结果按相关度降序排列
                    break
                ans_list.append((similar_ans[i]['_source']['o'], corel))
        ans_list = set(ans_list)  # 去重
        ans_list = list(ans_list)
        return ans_list

    #单位换算,数值型数据换算成标准单位，暂时先处理一些常用单位及日期
    #常用单位返回思路：返回一个元组（数值，标准单位），目前实现了长度 质量 面积单位
    # 日期返回思路：公元前的年份用负数表示，如公元前221-9-8 写法 返回((-221,9,8),('年'，'月'，'日'))，如果年月日都有，单位为('年'，'月'，'日')，如果哪个没有，相应单位位置为'',数值位置为0，如1949年5月返回((1949,5,0)('年','月','')
    def value_normolize(self, s):
        if isinstance(s, str): #输入s为一个字符串
            if s == "" or s == ():
                result = (0, 'None')
            tmp = ()
            result = (s, "str")
            reference_day = datetime.datetime(1970, 1, 1)

            # zhj
            if len(s) > 0:
                if s[-1] == '}':
                    ss = s.split('{')
                    if len(ss) >= 2:
                        count = float(ss[0])
                        unit = ss[-1].split('}')[0]
                        return count, '{'+unit+'}'

            if re.search(r"\d+-\d+-\d+",s):  # 公元前221-9-8 写法 返回((-221,9,8),('年'，'月'，'日'))
                date = re.findall(r"\d+\.?\d*", s)
                for i in range(len(date)):
                    date[i] = int(date[i])
                if ('公元前' or 'B.C.') in s:
                    date[0] = -date[0]
                result = ((date[0], date[1], date[2]), ('年', '月', '日'))

            elif (len(s)>0 and s[0]>='0'and s[0]<='9'):  # 数值 单位 数值 单位  写法
                num = re.findall(r"\d+\.?\d*", s)
                unit = re.findall(r"[^\.0-9]+", s)
                for i in range(len(num)):
                    num[i] = float(num[i])
                if ('年' or '月' or '日') in unit:  # 日期
                    for i in range(len(num)):
                        num[i] = int(num[i])
                    if ('公元前' or 'B.C.') in s:
                        num[0] = -num[0]
                        unit = unit[1:]
                    if ('年' and '月' and '日') in unit:
                        result = (tuple(num), tuple(unit))

                    elif ('年' and '月') in unit:
                        result = ((num[0], num[1], 0), ('年', '月', ''))

                    elif ('月' and '日') in unit:
                        result = ((0, num[0], num[1]), ('', '月', '日'))

                    elif ('年') in unit:
                        result = ((num[0], 0, 0), ('年', '', ''))

                    elif ('月') in unit:
                        result = ((0, num[0], 0), ('', '月', ''))

                    elif ('日') in unit:
                        result = ((0, 0, num[0]), ('', '', '日'))
                elif len(num)==len(unit):
                    count=0
                    stdard=''
                    for i in range(len(unit)):
                        #长度单位
                        if unit[i]==('纳米' or 'nm'):
                            count+=num[i]/1000000000
                            stdard = '米'
                        if unit[i]==('微米' or 'um'):
                            count+=num[i]/1000000
                            stdard = '米'
                        if unit[i]==('毫米' or 'm'):
                            count+=num[i]/1000
                            stdard = '米'
                        if unit[i]==('厘米' or 'cm'):
                            count+=num[i]/100
                            stdard='米'
                        if unit[i]==('分米' or 'dm'):
                            count+=num[i]/100
                            stdard='米'
                        if unit[i]==('里'):
                            count+=num[i]*500
                            stdard='米'
                        if unit[i]==('米' or 'm'):
                            count+=num[i]
                            stdard = '米'
                        if unit[i]==('千米' or 'km' or '公里'):
                            count+=num[i]*1000
                            stdard = '米'
                        #面积单位
                        if unit[i]==('平方毫米' or 'mm2'):
                            count+=num[i]/1000000
                            stdard = '平方米'
                        if unit[i]==('平方厘米' or 'm2'):
                            count+=num[i]/10000
                            stdard = '平方米'
                        if unit[i]==('平方分米' or 'dm2'):
                            count+=num[i]/100
                            stdard = '平方米'
                        if unit[i]==('平方米' or 'm2'):
                            count+=num[i]
                            stdard = '平方米'
                        if unit[i]=='公顷':
                            count+=num[i]*10000
                            stdard = '平方米'
                        if unit[i]==('平方千米' or 'km2'):
                            count+=num[i]*1000000
                            stdard = '平方米'
                        if unit[i]==('平方英里'):
                            count+=num[i]*2589988.11034
                            stdard = '平方米'

                        #体积单位
                        if unit[i]==('立方厘米' or 'cm3' or 'mL' or '毫升'):
                            count+=num[i]*1000000
                            stdard = '立方米'
                        if unit[i] ==( '立方分米' or 'dm3' or '升' or 'L'):
                            count += num[i] * 1000
                            stdard = '立方米'
                        if unit[i] == ('立方米' or 'm3') :
                            count += num[i]
                            stdard = '立方米'
                        #质量单位
                        if unit[i]==('毫克' or 'mg'):
                            count+=num[i]/1000000
                            stdard = '千克'
                        if unit[i]==('克' or 'g'):
                            count+=num[i]/1000
                            stdard = '千克'
                        if unit[i]=='斤' :
                            count+=num[i]*0.5
                            stdard = '千克'
                        if unit[i]==('千克' or 'kg'):
                            count+=num[i]
                            stdard = '千克'
                        if unit[i]==('吨' or 't'):
                            count+=num[i]*1000
                            stdard = '千克'
                        #货币单位 暂不做zhj
                        if unit[i]=='瑞典克朗':
                            count+=num[i]
                            stdard = unit[i]
                            # count+=num[i]*0.7841
                            # stdard = '元'
                        if unit[i]==('欧元' or '欧'):
                            count+=num[i]
                            stdard = unit[i]
                            # count+=num[i]*8.0983
                            # stdard = '元'
                            #未完待续…
                    result=(count,stdard)
                elif (len(unit)==0) and (len(num)==1):  #没有单位的情况
                    result = (num[0], 'IllegalUnit')
        elif isinstance(s, list):
            result = []
            for i in range(len(s)):
                buffer = self.value_normolize(s[i])
                result.append(buffer)
        return result





