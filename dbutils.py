# -*- coding: utf-8 -*-
import json
import jieba
from han import get_lang, f2j, j2f


def pro_fulltext(eqldb, record):
    record2 = record
    qv_add = {}
    distinct_words = []
    fields_spo = ['s', 'p', 'o']
    fields = fields_spo
    qv = record2.get('qv')
    if qv is not None:
        fields += list(qv.keys())
    i = 1
    for field in fields:
        if field in fields_spo:
            fulltext = str(record2.get(field))
        else:
            fulltext = str(qv.get(field))
        words = jieba.cut(widx2label(eqldb,fulltext,'all'), cut_all=True)
        for word in words:
            if word not in distinct_words:
                qv_add['分词' + str(i)] = word
                i += 1
                distinct_words.append(word)
    record2['qv'].update(qv_add)
    return record2


def propexpr2wid(eqldb, propexpr):
    labels = propexpr.split('.')
    wids = list(map(lambda x: label2wid(eqldb, x), labels))
    return '.'.join(wids)


def propexpr2label(eqldb, propexpr, lang):
    wids = propexpr.split('.')
    labels = list(map(lambda x: wid2label(eqldb, x, lang), wids))
    return '.'.join(labels)


def name2wid(eqldb, name):
    if name == '' or name is None:
        return []
    namex = f2j(name)#繁体字转化成简体字
    wids = []
    o = [namex]
    name2 = j2f(namex)
    if name2 not in o:
        o.append(name2) #将繁体字也加到列表里
    records = eqldb.search_simple('', ['_label', '_alias'], o)
    for rec in records:
        wid = rec['_source']['s']
        wids.append(wid)
    if len(wids) == 0:
        wids = [name]
    return wids


def wid2name(eqldb, wid):
    if wid == '' or wid is None:
        return []
    names = []
    records = eqldb.search_simple([wid], ['_label'], '')  # , '_alias'
    for rec in records:
        name = rec['_source']['o']
        names.append(f2j(name))
    if len(names) == 0:
        names = [wid]
    return names


def label2wid(eqldb, labelx):
    if labelx == '_别名' or labelx == '_alias':
        return '_alias'
    if labelx == '_标签' or labelx == '_label':
        return '_label'
    if labelx == '_描述' or labelx == '_description':
        return '_des'
    if labelx == '_频次' or labelx == '_frequency':
        return '_freq'
    if labelx == '_语种' or labelx == '_language':
        return '_lang'

    labelx = f2j(labelx)

    wids = []
    label = labelx.split('_')[0]
    records = eqldb.search_simple('', ['_label'], [label, j2f(label)])
    for rec in records:
        wid = rec['_source']['s']
        des = rec['_source']['qv'].get('_des')
        if des is None:
            des = ''
        des = f2j(des)
        if des == '':
            records2 = eqldb.search_simple([wid], ['_label'], '')
            for rec2 in records2:
                des1 = rec2['_source']['qv'].get('_des')
                if des1 is None:
                    des1 = ''
                des = f2j(des1)
                if des != '':
                    break
        if des == '':
            des = wid
        if labelx == label or labelx == label + '_' + des or labelx == label + '_' + wid:
            wids.append(wid)
    records = eqldb.search_simple('', ['_alias'], [labelx, j2f(labelx)])
    for rec in records:
        wid = rec['_source']['s']
        wids.append(wid)
    records = eqldb.search_simple(wids, ['_freq'], '')
    wid_max = ''
    freq_max = 0
    for rec in records:
        freq = int(rec['_source']['o'])
        if freq > freq_max:
            wid_max = rec['_source']['s']
            freq_max = freq
    if wid_max == '':
        return labelx
    else:
        if labelx == '日期':  # todo 必须wid_max不空
            wid_max = 'P585'
        return wid_max


def label2des(eqldb, label):
    lang = get_lang(label)
    label = f2j(label)
    wid = label2wid(eqldb, label)
    dess = []
    records = eqldb.search_simple([wid], ['_label'], '')
    for rec in records:
        des = rec['_source']['qv'].get('_des')
        if des is None:
            des = ''
        des = f2j(des)
        if lang == rec['_source']['qv'].get('_lang'):
            dess.append(des)
    # records = eqldb.search_simple('', ['_alias'], [label]) # todo
    # for rec in records:
    if len(dess) == 0:
        return ""
    elif len(dess) == 1:
        return dess[0]
    else:
        return dess


def label2dup(eqldb, label):
    label = f2j(label)
    wid = label2wid(eqldb, label)
    dups = []
    records = eqldb.search_simple('', ['_label'], [label])
    for rec in records:
        wid2 = rec['_source']['s']
        des = rec['_source']['qv'].get('_des')
        if des is None:
            des = ''
        des = f2j(des)
        if des == '':
            records2 = eqldb.search_simple([wid2], ['_label'], '')
            for rec2 in records2:
                des = f2j(rec2['_source']['qv']['_des'])
                if des != '':
                    break
        if des == '':
            des = wid2
        if wid != wid2 and des != '':
            dups.append(label+'_'+des)
    # records = eqldb.search_simple('', ['_alias'], [label]) # todo
    # for rec in records:
    return dups


def wid2label(eqldb, wid, lang):
    if len(wid) > 0:
        if wid[0] == '?' or wid[0] == '？':
            return wid
        if wid[0] == '_' and lang == 'zh':
            if wid == '_alias':
                return '_别名'
            if wid == '_label':
                return '_标签'
            if wid == '_des':
                return '_描述'
            if wid == '_freq':
                return '_频次'
            if wid == '_lang':
                return '_语种'
        if wid[0] == '_' and lang == 'en':
            if wid == '_alias':
                return '_alias'
            if wid == '_label':
                return '_label'
            if wid == '_des':
                return '_description'
            if wid == '_freq':
                return '_frequency'
            if wid == '_lang':
                return '_language'
    label = wid
    des = ''
    des_all = []
    if len(wid) >= 2:
        if (wid[0] == 'P' or wid[0] == 'Q') and wid[1:].isdigit():
            records = eqldb.search_simple([wid], ['_label'], '')
            for rec in records:
                label2 = rec['_source']['o']
                lang2 = rec['_source']['qv'].get('_lang')
                des2 = str(rec['_source']['qv'].get('_des'))
                if lang == 'all':
                    label += label2
                else:
                    if lang2 == lang:
                        label = label2
                        des = des2
                    elif label == wid:
                        label = label2
                    if des2 != '':
                        des_all.append(des2)
            #caches[wid+'_'+lang] = label
    if des == '' and len(des_all) > 0:
        des = des_all[0]
    if label != wid:
        wid2 = label2wid(eqldb, label)
        if wid2 != wid:
            label += '_' + des
    return label

widx2label_cache = {}

def widx2label(eqldb, widx, lang):
    if type(widx) == str and len(widx) > 0:
        if False:  # widx[0] == '{' and widx[-1] == '}': #json
            js = json.loads(widx)
            kv = list(js.items())
            di = {}
            di[widx2label(eqldb,kv[0][0], lang)] = widx2label(eqldb,kv[0][1], lang)
            return json.dumps(di,ensure_ascii=False)
        else:
            key = eqldb.db + '_' + widx + '_' + lang
            label = widx2label_cache.get(key)
            if label is None:
                label = ''
                if len(widx) > 0:
                    if widx[-1] == '}':
                        ss = widx.split('{')
                        if len(ss) >= 2:
                            wid = ss[-1].split('}')[0]
                            labelx = wid2label(eqldb, wid, lang)
                            if labelx != wid:
                                label = ss[0] + labelx
                if label == '':
                    label = wid2label(eqldb, widx, lang)
                widx2label_cache[key] = label
            return label
    if type(widx) == tuple:
        res = ()
        for w in widx:  # ('{"P1686": "Q1196539"}', '{"P805": "Q508390"}', '{"P585": "1939"}')
            if w.startswith('{') and w.endswith('}'):
                w1 = eval(w)
                d = {}
                for w2 in w1:
                    d[widx2label(eqldb,w2, lang)] = widx2label(eqldb,w1.get(w2), lang)
                res += (str(d),)
            else:
                res += (widx2label(eqldb,w, lang),)
        return res

    return widx
