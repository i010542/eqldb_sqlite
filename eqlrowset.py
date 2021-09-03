# -*- coding: utf-8 -*-
import re
from eqlutils import utils
from functools import cmp_to_key
import dbutils

class Eqlrowset:
    record_of_prop_expr = ""

    def __init__(self, eqldata, rowset):
        self.rowset = rowset
        self.eqldata = eqldata

    def find_index(self, prop_expr, rowset):
        if '.' in prop_expr:
            target_expr = prop_expr.split('.')[0]
        else:
            target_expr = prop_expr
        target_index = -1   # 在rowset的每一项中都找不到这个变量时，返回-1
        for i in range(0, len(rowset)):
            for j in range(0, len(rowset[i])):
                if target_expr == rowset[i][j][0]:
                    target_index = j
        return target_index

    def filter_match(self, prop_expr, value,lang, callback=None):
        my_rowset = []
        new_utils = utils()
        n = len(self.rowset)
        if '.' in prop_expr:
            target_index = self.find_index(prop_expr, self.rowset)
        for i in range(0, len(self.rowset)):
            if '.' not in prop_expr:    # prop_expr ::= ?x | ?y ...
                for j in range(0, len(self.rowset[i])):
                    if self.rowset[i][j][0] == prop_expr:
                        if isinstance(self.rowset[i][j][2], str):
                            true_or_false = new_utils.match(dbutils.widx2label(self.eqldata, self.rowset[i][j][2], lang), value)
                        elif isinstance(self.rowset[i][j][2], tuple):
                            true_or_false = False
                            for k in range(0, len(self.rowset[i][j][2])):
                                if new_utils.match(dbutils.widx2label(self.eqldata, self.rowset[i][j][2][k],lang), value) == True:
                                    true_or_false = True
                        if true_or_false == True:
                            if callback != None:
                                callback(i+1, n, self.rowset[i])
                            my_rowset += [self.rowset[i],]
            else:   # prop_expr ::= ?x.x1.x2 | ?y.x1 ...
                buffer = new_utils.get_o(self.eqldata, self.rowset[i][target_index][2], prop_expr, 1)
                if isinstance(buffer, str):
                    true_or_false = new_utils.match(buffer, value)
                elif isinstance(buffer, tuple) or isinstance(buffer, list):
                    true_or_false = False
                    for k in range(0, len(buffer)):
                        if new_utils.match(dbutils.widx2label(self.eqldata, buffer[k],lang), value) == True:
                            true_or_false = True
                if true_or_false == True:
                    if callback != None:
                        callback(i+1, n, self.rowset[i])
                    my_rowset += [self.rowset[i],]
        if callback != None:
            callback(1, 1, None)
        return my_rowset

    def groupby(self, prop_expr, callback=None):
        new_utils = utils()
        my_rowset = []
        number = 0
        buffer_index_dict = dict()
        target_index = self.find_index(prop_expr, self.rowset)

        n = len(self.rowset)

        for i in range(0, len(self.rowset)):
            buffer = new_utils.get_o(self.eqldata, self.rowset[i][target_index][2], prop_expr)
            if buffer not in buffer_index_dict.keys():
                buffer_index_dict[buffer] = number
                my_rowset.append([])
                my_rowset[number].append([((prop_expr, '', buffer),)])
                my_rowset[number].append([])
                my_rowset[number][1] += (self.rowset[i],)
                number += 1
                if callback != None:
                    callback(i + 1, n, None)
            else:
                my_rowset[buffer_index_dict[buffer]][1] += (self.rowset[i],)
                if callback != None:
                    callback(i + 1, n, None)
        if callback != None:
            for i in range(0, len(my_rowset)):
                new_header = [
                    ((my_rowset[i][0][0][0][0] + '.group', my_rowset[i][0][0][0][1], my_rowset[i][0][0][0][2]),)]
                callback(i + 1, len(my_rowset), new_header[0])  #zhj add [0]

                for j in range(0, len(my_rowset[i][1])):
                    callback(int(i + 1), len(my_rowset), my_rowset[i][1][j])
        return my_rowset

    def mysort(self, rowset_a, rowset_b):
        new_utils = utils()
        prop_expr = self.record_of_prop_expr
        target_index = self.find_index(prop_expr, [rowset_a,])

        info_a = new_utils.value_normolize(new_utils.get_o(self.eqldata, self.tuple2str(rowset_a[target_index][2]), prop_expr, 0))
        info_b = new_utils.value_normolize(new_utils.get_o(self.eqldata, self.tuple2str(rowset_b[target_index][2]), prop_expr, 0))

        # 预处理
        if info_a[1] == 'str':
            info_b = (str(info_b[0]), "str")
        if info_b[1] == 'str':
            info_a = (str(info_a[0]), "str")
        if info_a[1] == '':
            info_a = ("", "")
        if info_b[1] == '':
            info_b = ("", "")

        if ('年' or '月' or '日') in info_a[1] or ('年' or '月' or '日') in info_b[1]:        # 说明处理的是日期
            if info_a[1] == 'IllegalUnit':
                info_a = ((int(info_a[0]), 1, 1), ('年', '月', '日'))
            if info_b[1] == 'IllegalUnit':
                info_b = ((int(info_b[0]), 1, 1), ('年', '月', '日'))

            if info_a[0][0] < info_b[0][0]:
                return 1
            elif info_a[0][0] > info_b[0][0]:
                return -1
            else:  # 比较月
                if info_a[0][1] < info_b[0][1]:
                    return 1
                elif info_a[0][1] > info_b[0][1]:
                    return -1
                else:  # 比较日
                    if info_a[0][2] < info_b[0][2]:
                        return 1
                    else:
                        return -1
        else:                                           # 处理的不是日期
            if info_a[1] == info_b[1]:                  # 比较的单位相同
                pass
            else:  # 比较的单位不同
                info_a = (str(info_a[0]), info_a[1])
                info_b = (str(info_b[0]), info_b[1])

            if info_a[0] < info_b[0]:
                return 1
            else:
                return -1

    def orderby(self, prop_expr, order='DESC', callback=None):      # DESC: 降序(default);  ASC:升序
        new_utils = utils()
        self.record_of_prop_expr = prop_expr
        if isinstance(self.rowset[0], list):                        # 说明是groupby后的rowset(特殊格式)
            self.rowset = self.rowset[1]

        my_rowset = self.rowset
        if order == 'DESC' or order == 'desc':
            my_rowset.sort(key=cmp_to_key(self.mysort))
        elif order == 'ASC' or order == 'asc':
            my_rowset.sort(key=cmp_to_key(self.mysort), reverse=True)
        n = len(my_rowset)
        if callback != None:
            for i in range(n):
                callback(i+1, n, my_rowset[i])
        return my_rowset

    def judge_if_var_in_rowseti(self, prop_expr, rowseti):
        if '.' in prop_expr:
            target_expr = prop_expr.split('.')[0]
        else:
            target_expr = prop_expr

        for m in range(len(rowseti)):
            if rowseti[m][0] == target_expr:
                return True
        return False

    def get_column(self, prop_expr, callback=None):
        if isinstance(self.rowset[0], list):        # 说明是groupby后的rowset(特殊格式)
            self.rowset = self.rowset[1]
        my_rowset = []
        new_utils = utils()
        target_index = self.find_index(prop_expr, self.rowset)

        if target_index == -1:
            return []
        else:
            count = 0
            n = len(self.rowset)
            for i in range(0, len(self.rowset)):
                if self.judge_if_var_in_rowseti(prop_expr, self.rowset[i]) == True:
                    if self.rowset[i][target_index][2] == () or self.rowset[i][target_index][2] == "":
                        pass
                    else:
                        my_rowset.append(())
                        buffer1 = ()
                        buffer1 += ((prop_expr),)
                        if '.' not in prop_expr:  # prop_expr ::= ?x | ?y ...
                            buffer1 += (self.rowset[i][target_index][1],)
                            buffer1 += (self.rowset[i][target_index][2],)
                        else:  # prop_expr ::= ?x.x1.x2 | ?y.x1 ...
                            buffer1 += ((''),)
                            buffer = new_utils.get_o(self.eqldata, self.rowset[i][target_index][2], prop_expr)
                            buffer1 += ((buffer),)
                        if callback != None:
                            callback(i+1, n, (buffer1),)
                        my_rowset[count] += (buffer1,)
                        count += 1
                else:
                    if callback is not None:
                        callback(i+1, n, None)

            my_rowset = list(set(my_rowset))       # 去重

        return my_rowset

    def judge_same_unit(self, unit1, unit2):
        if '年' or '月' or '日' in unit1:
            type_unit1 = 'date'
        else:
            type_unit1 = 'unit1[0]'
        if '年' or '月' or '日' in unit1:
            type_unit2 = 'date'
        else:
            type_unit2 = 'unit1[0]'
        if type_unit1 == type_unit2:
            return True
        else:
            return False

    def filter_prop(self, prop_expr, oper, value, callback=None):
        new_utils = utils()
        my_rowset = []
        n = len(self.rowset)
        if '.' in prop_expr:
            target_index = self.find_index(prop_expr, self.rowset)
        for i in range(0, len(self.rowset)):
            if '.' not in prop_expr: # prop_expr ::= ?x | ?y ...
                for j in range(0, len(self.rowset[i])):
                    if self.rowset[i][j][0] == prop_expr:
                        buffer = self.rowset[i][j][2]
            else: # prop_expr ::= ?x.x1.x2 | ?y.x1 ...
                buffer = new_utils.get_o(self.eqldata, self.rowset[i][target_index][2], prop_expr)
                # '=' | '!=' | '>' | '>=' | '<' | '<='
            if oper == '=':
                buffer = new_utils.get_o(self.eqldata, self.rowset[i][target_index][2], prop_expr, 1)
                if new_utils.value_normolize(value) in new_utils.value_normolize(buffer):
                    if callback != None:
                        callback(i+1, n, self.rowset[i])
                    my_rowset += [self.rowset[i]]
                else:
                    if callback != None:
                        callback(i + 1, n, None)
            elif oper == '!=':
                buffer = new_utils.get_o(self.eqldata, self.rowset[i][target_index][2], prop_expr, 1)
                if new_utils.value_normolize(value) not in new_utils.value_normolize(buffer):
                    if callback != None:
                        callback(i+1, n, self.rowset[i])
                    my_rowset += [self.rowset[i]]
                else:
                    if callback != None:
                        callback(i + 1, n, None)
            else:
                if isinstance(buffer, tuple):
                    buffer = buffer[0]
                normolized_buffer = new_utils.value_normolize(buffer)
                normolized_value = new_utils.value_normolize(value)
                vitual_rowset = [(('?x','',buffer),),(('?x','',value),)]
                self.record_of_prop_expr = '?x'
                vitual_rowset.sort(key=cmp_to_key(self.mysort))     #大的在前面
                if oper == '>':
                    if vitual_rowset[0] != vitual_rowset[1] and vitual_rowset[0][0][2] == buffer:
                        if callback != None:
                            callback(i+1, n, self.rowset[i])
                        my_rowset += [self.rowset[i]]
                    else:
                        if callback != None:
                            callback(i+1, n, None)
                elif oper == '>=':
                    if vitual_rowset[0] == vitual_rowset[1] or vitual_rowset[0][0][2] == buffer:
                        if callback != None:
                            callback(i+1, n, self.rowset[i])
                        my_rowset += [self.rowset[i]]
                    else:
                        if callback != None:
                            callback(i+1, n, None)
                elif oper == '<':
                    if vitual_rowset[0] != vitual_rowset[1] and vitual_rowset[0][0][2] == value:
                        if callback != None:
                            callback(i+1, n, self.rowset[i])
                        my_rowset += [self.rowset[i]]
                    else:
                        if callback != None:
                            callback(i+1, n, None)
                elif oper == '<=':
                    if vitual_rowset[0] == vitual_rowset[1] or vitual_rowset[0][0][2] == value:
                        if callback != None:
                            callback(i+1, n, self.rowset[i])
                        my_rowset += [self.rowset[i]]
                    else:
                        if callback != None:
                            callback(i+1, n, None)
        return my_rowset

    def judge_single_var_rowset(self, value1, oper, value2):
        value1 = str(value1)
        value2 = str(value2)
        if oper == '=':
            oper = '=='
        expression = value1 + oper + value2
        judge = eval(expression)
        return judge

    def filter_func(self, func_name, prop_expr, var_name, oper, value, callback=None):
        my_rowset = []
        if var_name == "":      # 不需要计算?y of ?x
            pass
        elif var_name != "":    # 需要计算?y of ?x
            new_rowset = Eqlrowset(self.eqldata, self.rowset)
            grouped_rowset = new_rowset.groupby(var_name)  # 先按照var_name分组

            n = len(grouped_rowset)

            for i in range(len(grouped_rowset)):
                one_of_rowset = grouped_rowset[i][1]    # 分组后的一个子rowset
                new_rowset1 = Eqlrowset(self.eqldata, one_of_rowset)
                get_prop_expr_column = new_rowset1.get_column(prop_expr)    # 取出子rowset的prop_expr列

                new_rowset2 = Eqlrowset(self.eqldata, get_prop_expr_column)
                # 对prop_expr列进行判断，如果符合要求，就把子rowset的var_name列(经去重)加入my_rowset
                if func_name == 'COUNT' or func_name == 'count':
                    if new_rowset2.judge_single_var_rowset(len(get_prop_expr_column), oper, value) == True:
                        if callback != None:
                            callback(i+1, n, new_rowset1.get_column(var_name))
                        my_rowset += new_rowset1.get_column(var_name)
                else:
                    judge = re.search("""\d+\.?\d*""", get_prop_expr_column[0][0][2])
                    if judge:
                        if func_name == 'AVG' or func_name == 'avg':
                            sum = 0
                            for i in range(0, len(get_prop_expr_column)):
                                sum += int(get_prop_expr_column[i][0][2])
                            avg = sum / len(get_prop_expr_column)
                            if new_rowset2.judge_single_var_rowset(avg, oper, value) == True:
                                if callback != None:
                                    callback(i+1, n, new_rowset1.get_column(var_name))
                                my_rowset += new_rowset1.get_column(var_name)
                        elif func_name == 'SUM' or func_name == 'sum':
                            sum = 0
                            for i in range(0, len(get_prop_expr_column)):
                                sum += int(get_prop_expr_column[i][0][2])
                            if new_rowset2.judge_single_var_rowset(sum, oper, value) == True:
                                if callback != None:
                                    callback(i+1, n, new_rowset1.get_column(var_name))
                                my_rowset += new_rowset1.get_column(var_name)
                        elif func_name == 'MAX' or func_name == 'max':
                            get_prop_expr_column.sort(key=lambda s: s[0][2])
                            if new_rowset2.judge_single_var_rowset(get_prop_expr_column[-1], oper, value) == True:
                                if callback != None:
                                    callback(i+1, n, new_rowset1.get_column(var_name))
                                my_rowset += new_rowset1.get_column(var_name)
                        elif func_name == 'MIN' or func_name == 'min':
                            get_prop_expr_column.sort(key=lambda s: s[0][2])
                            if new_rowset2.judge_single_var_rowset(get_prop_expr_column[0], oper, value) == True:
                                if callback != None:
                                    callback(i+1, n, new_rowset1.get_column(var_name))
                                my_rowset += new_rowset1.get_column(var_name)
                    else:
                        pass
        return my_rowset

    def tuple2str(self, sth):
        if sth == ():
            return ""
        if isinstance(sth, tuple):
            return sth[0]
        elif isinstance(sth, str):
            return sth
        else:
            return ""

    def function(self, result_var, func_name, prop_expr, var_name="", callback=None):
        # var_name有效格式: ?x | ?x.x1 | ?x.x1.x2...
        # var_name(default): None
        new_utils = utils()
        if var_name == "":
            my_rowset = []
            my_rowset.append(())
            buffer_rowset = self.get_column(prop_expr)
            if buffer_rowset == []:
                return []
            else:
                if func_name == 'COUNT' or func_name == 'count':
                    my_rowset[0] = ((result_var, '', str(len(buffer_rowset))),)
                    if callback is not None:
                        callback(1,1,my_rowset[0])
                else:
                    judge_num = re.search("""\d+\.?\d*""", self.tuple2str(buffer_rowset[0][0][2]))
                    if judge_num:
                        if func_name == 'AVG' or func_name == 'avg':
                            sum = 0
                            for i in range(0, len(buffer_rowset)):
                                if callback is not None:
                                    callback(i, len(buffer_rowset), None)
                                value_and_unit = new_utils.value_normolize(self.tuple2str(buffer_rowset[i][0][2]))
                                value = value_and_unit[0]
                                unit = value_and_unit[1]
                                sum += value
                            avg = sum / len(buffer_rowset)
                            my_rowset[0] = ((result_var, '',  str(avg)+unit),)
                            if callback is not None:
                                callback(i+1, i+1, my_rowset[0])
                        elif func_name == 'SUM' or func_name == 'sum':
                            sum = 0
                            for i in range(0, len(buffer_rowset)):
                                if callback is not None:
                                    callback(i, len(buffer_rowset), None)
                                value_and_unit = new_utils.value_normolize(self.tuple2str(buffer_rowset[i][0][2]))
                                value = value_and_unit[0]
                                unit = value_and_unit[1]
                                sum += value
                            my_rowset[0] = ((result_var, '', str(sum)+unit),)
                            if callback is not None:
                                callback(i+1, i+1, my_rowset[0])
                        elif func_name == 'MAX' or func_name == 'max':
                            buffer_rowset.sort(key=cmp_to_key(self.mysort))
                            my_rowset[0] = ((result_var, '', str(self.tuple2str(buffer_rowset[0][0][2]))),)
                            if callback is not None:
                                callback(1, 1, my_rowset[0])
                        elif func_name == 'MIN' or func_name =='min':
                            buffer_rowset.sort(key=cmp_to_key(self.mysort))
                            my_rowset[0] = ((result_var, '', str(self.tuple2str(buffer_rowset[-1][0][2]))),)
                            if callback is not None:
                                callback(1, 1, my_rowset[0])
                    else:
                        pass
        elif var_name != "":
            # 先对rowset按var_name分组,构造rowset子集,再分别执行var_name == ""情况的语句
            my_rowset = []
            new_rowset = Eqlrowset(self.eqldata, self.rowset)
            grouped_rowset = new_rowset.groupby(var_name)#分组后的rowset
            n = len(grouped_rowset)
            for i in range(len(grouped_rowset)):
                # one_of_rowset = []#提取出的分组后的一个子rowset
                # for j in range(1, len(grouped_rowset[i])):
                one_of_rowset = grouped_rowset[i][1]
                new_rowset_object = Eqlrowset(self.eqldata, one_of_rowset)  # 用子rowset构造新的对象
                buffer_rowset = new_rowset_object.function(result_var, func_name, prop_expr)    # 对子rowset进行原函数操作
                str1 = buffer_rowset[0][0][0] # + ": " + str(var_name) + " = " + str(grouped_rowset[i][0][0][0][2])
                one_of_new_rowset = []
                one_of_new_rowset.append(())
                one_of_new_rowset[0] = ((str1, '', buffer_rowset[0][0][2]),)    # 子rowset函数操作后返回的rowset(提示信息有改变)
                # 将第二个变量值也放进结果rowset，zhaohj
                one_of_new_rowset[0] = (grouped_rowset[i][0][0][0], one_of_new_rowset[0][0])
                if callback != None:
                    callback(i+1, n, one_of_new_rowset[0])
                my_rowset += one_of_new_rowset  # 最终返回的rowset
        return my_rowset

    def get_common_properties(self, rowset):
        common_properties = []
        input1 = self.rowset[0]
        input2 = rowset[0]
        for i in range(len(input1)):
            buffer = ()
            variable = input1[i][0]    # '?x' | '?y' ...
            for j in range(len(input2)):
                if input2[j][0] == variable:
                    buffer += (variable,)
                    buffer += (self.find_index(variable, self.rowset),)
                    buffer += (self.find_index(variable, rowset),)
                    common_properties.append(buffer)
        return common_properties

    def intersect(self, rowset, callback=None, n=None, start=None):    # 交
        self.rowset = list(set(self.rowset))
        rowset = list(set(rowset))
        sign = 1    # sign: 0 ==>是函数直接调用 sign: 1 ==>是union调用
        if n is None:
            n = len(self.rowset)
            sign = 0
        my_rowset = []
        if (self.rowset == [()] or self.rowset == []) and (rowset == [()] or rowset == []):     #逻辑运算
        # [()]: TRUE
        # []: FALSE
            if [] in [self.rowset, rowset]:
                if callback is not None:
                    callback(1, 1, [])
                return []
            else:
                if callback is not None:
                    callback(1, 1, [()])
                return [()]
        else:   # rowset运算
            if len(rowset) > 0:
                common_properties = self.get_common_properties(rowset)
                list_common_properties_without_index = ()
                for i in range(len(common_properties)):
                    list_common_properties_without_index += (common_properties[i][0],)

                for i in range(len(self.rowset)):
                    for j in range(len(rowset)):
                        k = -1
                        for k in range(len(common_properties)):
                            if self.rowset[i][common_properties[k][1]] != rowset[j][common_properties[k][2]]:
                                break
                            else:
                                if k == len(common_properties) - 1:
                                    k += 1
                        if k == len(common_properties):
                            # 说明这一对ij组合【匹配】
                            # 先把rowset1全部属性加入,再将匹配的rowset2中有差别的属性加入
                            buffer = self.rowset[i]
                            for ii in range(len(rowset[j])):
                                if rowset[j][ii][0] in list_common_properties_without_index:
                                    pass
                                else:
                                    buffer += (rowset[j][ii],)

                            if callback is not None:
                                if sign == 0:
                                    callback(i+1, n, buffer)
                                elif sign == 1:
                                    callback(start+i, n, buffer)

                            my_rowset.append(buffer)

                        elif k == -1:   # 说明没有匹配元素（common_properties 长度为 0）直接相加
                            my_rowset = self.rowset + rowset
                        else:
                            if callback is not None:
                                if sign == 0:
                                    callback(i+1, n, None)
                                elif sign == 1:
                                    callback(start + i, n, None)
        return my_rowset

    def union(self, rowset, callback=None):    # 并
        self.rowset = list(set(self.rowset))
        rowset = list(set(rowset))
        # A并B = A交B + A差B + B差A
        if (self.rowset == [()] or self.rowset == []) and (rowset == [()] or rowset == []):     #逻辑运算
        #     [()]: TRUE
        #     []: FALSE
            if [()] in [self.rowset, rowset]:
                return [()]
            else:
                return []
        else:
            n = len(self.rowset) + len(self.rowset) + len(rowset)

            my_rowset = self.intersect(rowset=rowset, callback=callback, n=n, start=1)

            new_rowset_1 = Eqlrowset(self.eqldata, self.rowset)
            excluded_rowset_1 = new_rowset_1.exclude(rowset, callback=callback, n=n, start=len(self.rowset))
            len_excluded_rowset_1 = len(excluded_rowset_1)

            new_rowset_2 = Eqlrowset(self.eqldata, rowset)
            excluded_rowset_2 = new_rowset_2.exclude(self.rowset, callback=callback, n=n, start=2*len(self.rowset))
            len_excluded_rowset_2 = len(excluded_rowset_2)

            for i in range(len_excluded_rowset_1):
                my_rowset.append(excluded_rowset_1[i])

            for i in range(len_excluded_rowset_2):
                my_rowset.append(excluded_rowset_2[i])
        return my_rowset

    def exclude(self, rowset, callback=None, n=None, start=None):    # 差
        self.rowset = list(set(self.rowset))
        rowset = list(set(rowset))
        sign = 1    # sign: 0 ==>是函数直接调用
                    # sign: 1 ==>是union调用
        if n is None:
            n = len(self.rowset)
            sign = 0

        if self.rowset == [()]:
            if callback is not None:
                callback(1, 1, [])
            return []
        elif self.rowset == []:
            if callback is not None:
                callback(1, 1, [()])
            return [()]
        else:
            my_rowset = []
            common_properties = self.get_common_properties(rowset)
            list_common_properties_without_index = ()
            for i in range(len(common_properties)):
                list_common_properties_without_index += (common_properties[i][0],)

            for i in range(len(self.rowset)):
                pointer = 0
                for j in range(len(rowset)):
                    k = 0
                    while k < len(common_properties):
                        if self.rowset[i][common_properties[k][1]] != rowset[j][common_properties[k][2]]:
                            break
                        k += 1
                    if k == len(common_properties):     # 匹配到
                        pointer = 1
                if pointer == 0:
                    if callback is not None:
                        if sign == 0:
                            callback(i+1, n, self.rowset[i])
                        elif sign == 1:
                            callback(start+i, n, self.rowset[i])
                    my_rowset.append(self.rowset[i])
                else:
                    if callback is not None:
                        if sign == 0:
                            callback(i+1, n, None)
                        elif sign == 1:
                            callback(start+i, n, None)
        return my_rowset