# -*- coding:utf-8 -*-
"""
计算余弦相似性
1.在info中提取关键字信息；
2.根据出现频率为关键字排序——降序
3.手工挑选关键字（200个），保存到keyword.txt文件中
4.调用关键字将info信息转换为0 1分布
5.计算info信息的余弦相似性，保存到test.txt中
"""
import pymysql
import thulac
import re
from textrank4zh import TextRank4Keyword,TextRank4Sentence

thul = thulac.thulac()  #默认模式

# 指定数据库名和表名即从mysql中读取数据
def LoadData(db, sql):
    conn = pymysql.connect(host='172.16.10.161',port=3306,user='root',password='root',db=db,charset='utf8')
    with conn.cursor() as cur:
        cur.execute(sql)
        Result = cur.fetchall()
    return Result


# 获取事件具体信息
def GetEventInfo():
    #---------------------------------数据库名称---------------------------------
    db = "bbg"
    ##----------------------------指定数据库中的表名-----------------------------
    sql = "SELECT info FROM coupon_scheme "
    dataset = LoadData(db, sql)
    # print(dataset)
    # 将获取的事件具体信息的格式由元组改为列表
    DataSet = []
    for data in dataset:
        for i in data:
            DataSet.append(i)
    return DataSet

'''
#获取info中的关键字列表
def GetInfoElement(strings):
    Nouns = []
    Keys = []
    for string in strings:
        string = str(string)
        wordflags = thul.cut(string)
        pattern = re.compile(u"^[\u4e00-\u9fa5]+$")
        Noun = []
        for word, flag in wordflags:
            if pattern.search(word):
                if flag == 'n'
        tr4w = TextRank4Keyword()
        tr4w.analyze(string, window=2)
        Key = [i["word"] for i in tr4w.get_keywords(num=20)]

        # # 如果包含停用词，则删除
        # Noun = list(set(Noun).difference(set(StopWords)))
        # Key = list(set(Key).difference(set(StopWords)))

        Nouns.append(Noun)
        Keys.append(Key)
    print(Keys)
    # KeyWord = []
    # for i in Keys:
    #     KeyWord += i
    # KeyWord = list(set(KeyWord))
    # print(len(KeyWord))
    # print(Nouns)

    # return KeyWord
'''

# 调用人工挑选的关键字文本（含有200个关键字）
def GetKeyWord():
    keyword = []
    for line in open("keyword.txt", "r"):  # 设置文件对象并读取每一行文件
        keyword.append(line[:-1])  # 将每一行文件加入到list中)
    return keyword

# Info信息匹配关键字列表 => 0 1序列
def InfoMatchKeyword(Info,KeyWord):
    KeyNum = []
    for j in Info:
        j = str(j)
        KeyFlag = ""
        i = 0
        while i < len(KeyWord):
            if KeyWord[i] in j:
                KeyFlag += "1"
            else:
                KeyFlag += "0"
            i += 1
        KeyNum.append(KeyFlag)

    return KeyNum


# 计算余弦相似性
def ComputeSimilarity(bi_num):
    res = []
    for row in bi_num:
        for col in bi_num:
            m = 0
            numerator = 0
            while m < len(row):
                numerator += int(row[m]) * int(col[m])
                m += 1
            NormRow = 0
            NormCol = 0
            for i in row:
                i = int(i)
                NormRow += i ** 2
            for j in col:
                j = int(j)
                NormCol += j ** 2
            if NormRow == 0 or NormCol == 0:
                result = 0
            else:
                result = round(numerator / ((NormRow ** 0.5) * (NormCol ** 0.5)), 2)
            res.append(result)
    # print(res)


    with open('test2002.txt', 'w') as f:
        length = len(bi_num[0])
        count = 0
        for num in res:
            count += 1
            f.write(str(num).ljust(8, ' '))
            if count % length == 0:
                f.write("\n")
                continue

    return res

# 获取事件cpns_id
def GetCpnsid():
    #---------------------------------数据库名称---------------------------------
    db = "bbg"
    ##----------------------------指定数据库中的表名-----------------------------
    sql = "SELECT cpns_id FROM coupon_scheme "
    dataset = LoadData(db, sql)
    print(dataset)
    # 将获取的事件具体信息的格式由元组改为列表
    cpns_id = []
    for data in dataset:
        for i in data:
            cpns_id.append(i)

    return cpns_id


def InsertInfoToPymysql(dataset, cpns_id):
    length = len(dataset) ** 0.5
    length = int(length)
    conn = pymysql.connect(host='172.16.10.161',port=3306,user='root',password='root',db='tmp',charset='utf8')
    with conn.cursor() as cur:
        for i in range(length):
            for j in range(length):
                if dataset[length * i + j] != 0:
                    a = cpns_id[i]
                    b = cpns_id[j]
                    c = str(dataset[length * i + j])
                    # print(a)
                    # print(b)
                    # print(c)
                    # --------------------------------插入表名------------------------------------
                    sql = "INSERT INTO info_similarty(x, y, similarty) VALUES ("+a+", "+b+", "+c+")"
                    cur.execute(sql)
                else:
                    pass

# 从 coupon_scheme 中获取事件具体信息，包含 cpns_id 和 info
def GetCpnsidInfo():
    #---------------------------------数据库名称---------------------------------
    db = "bbg"
    ##----------------------------指定数据库中的表名-----------------------------
    sql = "SELECT cpns_id,info FROM coupon_scheme "

    dataset = LoadData(db, sql)
    cpns_id = []
    info = []
    for data in dataset:
        cpns_id.append(data[0])
        info.append(data[1])
    return cpns_id, info


# 对所有 已有的 info信息进行编码，将其改为0 1序列
def InfoMatchKeyword(Info,KeyWord):
    KeyNum = []
    for j in Info:
        j = str(j)
        KeyFlag = ""
        i = 0
        while i < len(KeyWord):
            if KeyWord[i] in j:
                KeyFlag += "1"
            else:
                KeyFlag += "0"
            i += 1
        KeyNum.append(KeyFlag)

    return KeyNum


# 将所有 已有的 info信息进行编码，然后插入到数据库中 tmp -> info_coding
def InsertInfoToPymysql(cpns_id, info, bi_num):
    length = len(cpns_id)
    conn = pymysql.connect(host='172.16.10.161',port=3306,user='root',password='root',db='tmp',charset='utf8')
    with conn.cursor() as cur:
        for i in range(length):
            # --------------------------------插入表名------------------------------------
            sql = "INSERT INTO info_coding(cpns_id, info, info_coding) VALUES ('%s', '%s', '%s')" % (cpns_id[i], info[i], bi_num[i])
            # sql = "INSERT INTO info_coding(cpns_id, info, info_coding) VALUES ("+str(cpns_id[i])+", "+str(info[i])+", "+str(bi_num[i])+")"
            cur.execute(sql)


# 对 新来的 info信息进行编码，即将其改为0 1序列
def NewInfoMatchKeyword(NewInfo,KeyWord):
    KeyFlag = ""
    i = 0
    while i < len(KeyWord):
        if KeyWord[i] in NewInfo:
            KeyFlag += "1"
        else:
            KeyFlag += "0"
        i += 1

    return KeyFlag

# 将 新来的 info信息进行编码，然后插入到数据库中 tmp -> info_coding
def InsertNewInfoToPymysql(cpns_id, info, bi_num):
    conn = pymysql.connect(host='172.16.10.161',port=3306,user='root',password='root',db='tmp',charset='utf8')
    with conn.cursor() as cur:
        # --------------------------------插入表名------------------------------------
        sql = "INSERT INTO info_coding(cpns_id, info, info_coding) VALUES ('%s', '%s', '%s')" % (cpns_id, info, bi_num)
        # sql = "INSERT INTO info_coding(cpns_id, info, info_coding) VALUES ("+str(cpns_id[i])+", "+str(info[i])+", "+str(bi_num[i])+")"
        cur.execute(sql)


# 从 info_coding 中获取事件具体信息，包含 cpns_id 和 info_coding
def GetCpnsidInfocoding():
    #---------------------------------数据库名称---------------------------------
    db = "tmp"
    ##----------------------------指定数据库中的表名-----------------------------
    sql = "SELECT cpns_id, info_coding FROM info_coding "

    dataset = CCS.LoadData(db, sql)
    cpns_id = []
    info_coding = []
    for data in dataset:
        cpns_id.append(data[0])
        info_coding.append(data[1])

    return cpns_id, info_coding


# 计算  新来的  info信息的余弦相似度
def ComputeNewSimilarity(new_bi_num, bi_num):
    newres = []
    for row in bi_num:
        m = 0
        numerator = 0
        while m < len(row):
            numerator += int(row[m]) * int(new_bi_num[m])
            m += 1
        NormRow = 0
        NormCol = 0
        for i in row:
            i = int(i)
            NormRow += i ** 2
        for j in new_bi_num:
            j = int(j)
            NormCol += j ** 2
        if NormRow == 0 or NormCol == 0:
            result = 0
        else:
            result = round(numerator / ((NormRow ** 0.5) * (NormCol ** 0.5)), 2)
        newres.append(result)

    # print(newres)
    # print(len(newres))
    return newres


# 将  新来的  info信息的余弦相似度结果插入到数据库中  tmp -> info_similarty
def InsertNewinfoToPymysql(cpns_id, new_cpns_id, new_info_sim):
    conn = pymysql.connect(host='172.16.10.161', port=3306, user='root', password='root', db='tmp', charset='utf8')
    with conn.cursor() as cur:
        for i in range(len(cpns_id)):
            if new_info_sim[i] != 0:
                # a = new_cpns_id
                # b = cpns_id[i]
                # c = str(dataset[length * i + j])
                # print(a)
                # print(b)
                # print(c)
                sql = "INSERT INTO info_similarty(x, y, similarty) VALUES ('%s', '%s', '%s')"  % (new_cpns_id, cpns_id[i], str(new_info_sim[i]))
                # sql = "INSERT INTO info_class2(x, y, similarty) VALUES (" + a + ", " + b + ", " + c + ")"
                cur.execute(sql)
            else:
                pass



