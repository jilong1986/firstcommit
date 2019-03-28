# -*- coding:utf-8 -*-

import pymysql
import thulac
import re
from textrank4zh import TextRank4Keyword,TextRank4Sentence


thul = thulac.thulac()  #默认模式

#读取停用词文件
def GetStopWord():
    words = [i.split(':')[0] for i in open('./title.txt','r',encoding='utf-8').readlines()]
    # Num = len(open('./title.txt','r',encoding='utf-8').readlines())
    # UpLimit = int(Num*0.95)
    # LowLimit = int(Num*0.1)
    # ValueWords = words[LowLimit:UpLimit]
    # StopWords = list(set(words).difference(ValueWords))
    StopWords = words
    return StopWords

#加载停用词
StopWords= GetStopWord()


# 指定数据库名和表名即从mysql中读取数据
def LoadData(db, sql):
    conn = pymysql.connect(host='172.16.10.161',port=3306,user='root',password='root',db=db,charset='utf8')
    with conn.cursor() as cur:
        cur.execute(sql)
        Result = cur.fetchall()
    return Result


# 获取事件具体信息
def GetEventInfo():
    db = "bbg"
    sql = "SELECT info FROM coupon_scheme "
    DataSet = LoadData(db, sql)
    # 将获取的具体信息元组改为列表形式
    INFO = []
    for i in DataSet:
        for j in i:
            INFO.append(j)
    return INFO

#获取info中的关键字列表
def GetInfoElement(strings):
    # Nouns = []
    Keys = []
    for string in strings:
        string = str(string)
        # wordflags = thul.cut(string)
        pattern = re.compile(u"^[\u4e00-\u9fa5]+$")
        # Noun = []
        # for word, flag in wordflags:
        #     if pattern.search(word):
        #         if flag == 'n':
        #             Noun.append(word)
        #         else:
        #             pass

        tr4w = TextRank4Keyword()
        tr4w.analyze(string, window=2)
        Key = [i["word"] for i in tr4w.get_keywords(num=20)]

        # 如果包含停用词，则删除
        # Noun = list(set(Noun).difference(set(StopWords)))
        Key = list(set(Key).difference(set(StopWords)))

        # Nouns.append(Noun)
        Keys.append(Key)

    KeyWord = []
    for i in Keys:
        KeyWord += i
    KeyWord = list(set(KeyWord))
    # print(len(KeyWord))

    return KeyWord

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

# 计算列表信息二维余弦相似性
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

    with open('test.txt', 'w') as f:
        length = len(bi_num[0])
        count = 0
        for num in res:
            count += 1
            f.write(str(num).ljust(8, ' '))
            if count % length == 0:
                f.write("\n")
                continue

# if __name__ == '__main__':
#     info = GetEventInfo()
#     Keyword = GetInfoElement(info)
#     bi_num = InfoMatchKeyword(info,Keyword)
#     ComputeSimilarity(bi_num)







