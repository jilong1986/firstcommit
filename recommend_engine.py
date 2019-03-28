from coupon import Coupon
import ComputeCosineSimilarity as ccs
from user import User
from item import Item
import pandas as pd
import pymysql
from configparser import ConfigParser
import os
from coupon import logging
from sqlalchemy import create_engine
import client_portrait

pymysql.install_as_MySQLdb()

#获取config配置文件
def getConfig(section, key):
    config = ConfigParser()
    # os.path.split(os.path.realpath(__file__))[0] 得到的是当前文件模块的目录
    path = os.path.split(os.path.realpath(__file__))[0] + '/configure.conf'
    config.read(path ,encoding='utf8')
    return config.get(section, key)

def export_feature(data):
    #TODO
    encoder_feature = data
    return encoder_feature
def find_sim_coupon(data):
    #TODO
    cpns_id_list  = []
    cpns_info_list = []
    for coupon_i in data['coupon']:
        cpns_id_list.append(coupon_i['cpns_id'])
        cpns_info_list.append(coupon_i['info'])
    return cpns_id_list

class Recommend_Engine:
    def __init__(self):
        self.datadir = None  # 文件路径
        self.coupon = None  # 优惠券
        self.user = None  # 用户
        self.item = None  #商品
        self.top_n = None # top_n数目
        self.filter_user = None #过滤用户的领用/使用数量
        self.filter_coupon = None #过滤优惠券的领用/发行数量
        self.filter_time = None #按时间过滤
        self.scheme_filename = None #优惠券定义表
        self.access_uses_filename = None #优惠券领用使用表
        self.member_filename = None #会员表
        self.use_order_filename = None #优惠券使用订单明细表
        self.custer_txndeail_filename01 = None #交易明细表1
        self.custer_txndeail_filename02 = None  # 交易明细表2
        self.mysql_conn = None #数据库连接配置
        self.coupon_scheme = None #优惠券定义表
        self.coupon_access_uses = None #优惠券领用使用表
        self.coupon_use_order = None #优惠券使用订单明细表
        self.member = None #会员表
        self.custer_txndeail = None #交易明细表
        self.coupon_brand_count = 0.1 # 优惠券-品牌偏好权重
        self.coupon_cat_count = 0.1  # 优惠券-品类偏好权重
        #========================
        self.mysql_engine = None
        # 优惠券领用表
        self.coupon_cust_access = None
        # 优惠券使用表
        self.coupon_cust_order = None
        # 优惠券-品牌表
        self.cpns_brand = None
        # 优惠券-品类表
        self.cpns_cat = None
        # 码表
        self.member_seibel = None
        # siebel_id_brand_Q表
        self.siebel_id_brand_id_q5 = None
        # siebel_id_subclass_P表
        self.siebel_id_subclass_id_q5 = None
        #读取coupon计算结果
        self.is_read_coupon_result = None
        self.is_read_brand_cat_result = None
        self.is_read_coupon_info_feature = None
        #========================
        self.__load_cfg()
        self.__load_data()
    def __load_data(self):
        logging.info('load data begin')
        self.coupon = Coupon(datadir=self.datadir,feature_list = self.feature_list, top_n=self.top_n)
        self.coupon.load_myqsl(self.mysql_engine,self.coupon_cust_access,self.coupon_cust_order,is_read_coupon_result=self.is_read_coupon_result)
        self.item = Item(datadir=self.datadir, top_n=self.top_n)
        self.item.load_mysql(self.mysql_engine, self.cpns_brand, self.cpns_cat,
                             is_read_brand_cat_result=self.is_read_brand_cat_result)
        self.user = User(datadir=self.datadir,coupon_brand_count = self.coupon_brand_count,coupon_cat_count =self.coupon_cat_count)
        self.user.load_mysql(self.mysql_engine, self.member_seibel, self.siebel_id_brand_id_q5, self.siebel_id_subclass_id_q5)
        # self.user.load_tsv(self.member_filename, self.custer_txndeail_filename01, self.custer_txndeail_filename02)
        if self.is_read_coupon_info_feature is '0':
            cpns_id = ccs.GetCpnsid()
            Info = ccs.GetEventInfo()
            KeyWord = ccs.GetKeyWord()
            bi_num = ccs.InfoMatchKeyword(Info, KeyWord)
            dataset = ccs.ComputeSimilarity(bi_num)
            ccs.InsertInfoToPymysql(dataset, cpns_id)

    def get_top_n_user(self,coupon_new ,n= None):
        #抽取特征、找相似的券列表(目前使用info字段)
        new_coupon_id = coupon_new['coupon'][0]['cpns_id']
        new_coupon_info = coupon_new['coupon'][0]['info']
        keyword = ccs.GetKeyWord()
        new_bi_num = ccs.NewInfoMatchKeyword(new_coupon_info, keyword)
        history_cpns_id, history_info_coding = ccs.GetCpnsidInfocoding()
        new_info_sim = ccs.ComputeNewSimilarity(new_bi_num, history_info_coding)
        ccs.InsertNewInfoToPymysql(history_cpns_id, new_coupon_id, new_info_sim)
        sim_table = pd.DataFrame([history_cpns_id,new_info_sim],columns=['cpns_id','info_sim'])
        sim_table.set_index('cpns_id',inplace=True)
        cpns_sim_list = list(sim_table['info_sim'].sort_values(ascending=False).index[0:10]) #10这个参数是否需要配置
        prediction = self.coupon.get_top_n_user(cpns_sim_list,n)
        #添加P/Q函数(关联品类、品牌偏好)
        top_n_user_prediction = self.user.get_top_n_user(prediction,n)
        # 客户画像
        siebel_id_list = self.user.get_seible_id_list(top_n_user_prediction.keys())
        client_portrait.InsertClientPortraitToPymysql(siebel_id_list)
        return top_n_user_prediction

    def get_and_eval_top_n_user(self, coupon_new, n=None):
        # 抽取特征、找相似的券列表(目前使用info字段)
        new_coupon_id = coupon_new['coupon'][0]['cpns_id']
        new_coupon_info = coupon_new['coupon'][0]['info']
        keyword = ccs.GetKeyWord()
        new_bi_num = ccs.NewInfoMatchKeyword(new_coupon_info, keyword)
        history_cpns_id, history_info_coding = ccs.GetCpnsidInfocoding()
        new_info_sim = ccs.ComputeNewSimilarity(new_bi_num, history_info_coding)
        ccs.InsertNewInfoToPymysql(history_cpns_id, new_coupon_id, new_info_sim)
        sim_table = pd.DataFrame([history_cpns_id, new_info_sim], columns=['cpns_id', 'info_sim'])
        sim_table.set_index('cpns_id', inplace=True)
        cpns_sim_list = list(sim_table['info_sim'].sort_values(ascending=False).index[0:10])  # 10这个参数是否需要配置
        prediction = self.coupon.get_top_n_user(cpns_sim_list, n)
        # 添加P/Q函数(关联品类、品牌偏好)
        top_n_user_prediction = self.user.get_top_n_user(cpns_sim_list,prediction, n)
        #客户画像
        siebel_id_list = self.user.get_seibel_id_list(top_n_user_prediction.keys())
        client_portrait.InsertClientPortraitToPymysql(siebel_id_list)
        #评价
        sql = "select * from " + self.coupon_cust_order + " where cpns_id= " + new_coupon_id
        c_table = pd.read_sql_query(sql, self.mysql_engine)
        actual = c_table['member_id'].unique()
        if len(actual) != 0:
            recall, precision = self.recall_precision(prediction, actual)
            return top_n_user_prediction, recall, precision
        else:
            logging.info('no this coupon %d recommend data',new_coupon_id)
            logging.info(self.coupon.recommend_top_n_user_table.index)
            return [],0,0
    def recall_precision(self,prediction_user,actual_user):
        hit = 0
        actual_all = len(actual_user)
        predit_all = len(prediction_user)
        logging.info('actual count %d ,predict count %d'%(actual_all,predit_all))
        for uid ,score in prediction_user.items():
            if uid in actual_user:
                hit += 1
        #返回召回率、准确率
        # print(hit)
        return hit/(actual_all*1.0),hit/(predit_all*1.0)

    def __load_cfg(self):
        logging.info('load cfg begin')
        self.datadir = getConfig("recommend_engine_user_coupon","datadir")
        self.top_n = getConfig("recommend_engine_user_coupon","top_n")
        self.filter_user = getConfig("recommend_engine_user_coupon","filter_user")
        self.filter_coupon = getConfig("recommend_engine_user_coupon","filter_coupon")
        self.filter_time = getConfig("recommend_engine_user_coupon","filter_time")
        self.scheme_filename = getConfig("recommend_engine_user_coupon","scheme_filename")
        self.access_uses_filename = getConfig("recommend_engine_user_coupon", "access_uses_filename")
        self.member_filename = getConfig("recommend_engine_user_coupon", "member_filename")
        self.use_order_filename = getConfig("recommend_engine_user_coupon", "use_order_filename")
        self.custer_txndeail_filename01 = getConfig("recommend_engine_user_coupon", "custer_txndeail_filename01")
        self.custer_txndeail_filename02 = getConfig("recommend_engine_user_coupon", "custer_txndeail_filename02")
        feature_list = {}
        feature_list['access_action_count'] = self.access_action_count = getConfig("recommend_engine_user_coupon", "access_action_count")
        feature_list['use_action_count'] = self.access_action_count = getConfig("recommend_engine_user_coupon", "use_action_count")
        self.coupon_brand_count = getConfig("recommend_engine_user_coupon", "coupon_brand_count")
        self.coupon_cat_count = getConfig("recommend_engine_user_coupon", "coupon_cat_count")
        if getConfig("recommend_engine_user_coupon", "base_user") is '1':
            feature_list['base'] = 'base_user'
        else:
            feature_list['base'] = 'base_item'
        self.feature_list = feature_list

        self.mysql_conn = pymysql.connect(host=getConfig("database", "host"), port=int(getConfig("database", "port")), user=getConfig("database", "user"),
                                          password=getConfig("database", "password"), db=getConfig("database", "db"), charset=getConfig("database", "charset"))
        self.mysql_engine = create_engine('mysql+mysqldb://%s:%s@%s:%d/%s?charset=utf8'%(getConfig("database", "user"),getConfig("database", "password"),getConfig("database", "host"),int(getConfig("database", "port")),getConfig("database", "db")))
        self.coupon_scheme = getConfig("database", "coupon_scheme")
        self.coupon_cust_access = getConfig("database", "coupon_cust_access")
        self.coupon_cust_order = getConfig("database", "coupon_cust_order")
        self.cpns_brand = getConfig("database", "cpns_brand")
        self.cpns_cat = getConfig("database", "cpns_cat")
        self.member_seibel = getConfig("database", "member_seibel")
        self.siebel_id_brand_id_q5 = getConfig("database", "siebel_id_brand_id_q5")
        self.siebel_id_subclass_id_q5 = getConfig("database", "siebel_id_subclass_id_q5")
        self.is_read_coupon_result = getConfig("database", "is_read_coupon_result")
        self.is_read_brand_cat_result = getConfig("database", "is_read_brand_cat_result")
        self.is_read_coupon_info_feature = getConfig("database", "is_read_coupon_info_feature")
        logging.info('load cfg end')
