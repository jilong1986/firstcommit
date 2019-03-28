import pandas as pd
import logging
import os.path as path
from sklearn.metrics.pairwise import cosine_similarity



logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s: %(message)s: %(message)s', level=logging.INFO,filename='test.log')

class User:
    def __init__(self, datadir="./data/",coupon_brand_count=None,coupon_cat_count=None):
        self.datadir = datadir #文件路径
        self.user_table = None  #用户表（用于建立memberid—seibel_id对应表、抽取用户统计特征（年龄、性别、注册时间）
        self.custer_txndeail_table = None #消费表（用于建立用户-商品表）
        self.user_list = None   #用户列表
        self.item_list = None   #商品列表
        self.history_user_item_table = None #用户历史购买商品记录
        self.user_similarity = None #用户相似度矩阵
        self.item_similarity = None #商品相似度矩阵
        self.user_similar_table = None #用户相似度表
        self.coupon_brand_count = coupon_brand_count
        self.coupon_cat_count = coupon_cat_count
    def load_mysql(self,conn,member_seibel,siebel_id_brand_id_q5,siebel_id_subclass_id_q5):
        logging.info('load member_seible brand cat from mysql begin')
        # sql_member_seibel = "select * from " + member_seibel
        # member_seibel_table = pd.read_sql(sql_member_seibel, con=conn, columns=['seibel_id', 'member_id'])
        # sql_siebel_id_brand_id_q5 = "select * from " + siebel_id_brand_id_q5
        # siebel_id_brand_id_q5_table = pd.read_sql(sql_siebel_id_brand_id_q5, con=conn, columns=['seibel_id', 'concat_brandid'])
        # sql_siebel_id_subclass_id_q5 = "select * from " + siebel_id_subclass_id_q5
        # siebel_id_subclass_id_q5_table = pd.read_sql(sql_siebel_id_subclass_id_q5, con=conn,
        #                                           columns=['seibel_id', 'subclass_id'])
        self.conn = conn
        self.member_seibel = member_seibel
        self.siebel_id_brand_id_q5 = siebel_id_brand_id_q5
        self.siebel_id_subclass_id_q5 = siebel_id_subclass_id_q5
        logging.info('load brand cat - coupon from mysql end')
    def get_seibel_id(self,):

        pass
    #读取tsv文件数据
    def load_tsv(self, member_filename, custer_txndeail_filename01,custer_txndeail_filename02):
        logging.info(path.join(self.datadir, member_filename))
        custer_txndeail_table01 = pd.read_csv(path.join(self.datadir, custer_txndeail_filename01),nrows=30,usecols=['siebel_id','subclass'], sep='\t', header=0)
        custer_txndeail_table02 = pd.read_csv(path.join(self.datadir, custer_txndeail_filename02),nrows=30,usecols=['siebel_id','subclass'], sep='\t', header=0)
        user_table = pd.read_csv(path.join(self.datadir, member_filename), sep='\t', header=0)
        custer_txndeail_table = pd.concat([custer_txndeail_table01,custer_txndeail_table02])
        user_list = custer_txndeail_table['siebel_id'].unique()
        item_list = custer_txndeail_table['subclass'].unique()
        self.user_table = user_table
        self.custer_txndeail_table = custer_txndeail_table
        self.user_list = user_list
        self.item_list = item_list
        self.__save_to_user_item_table()
    #建立用户历史商品购买表
    def __save_to_user_item_table(self):
        history_user_item_table = pd.DataFrame(index=self.user_list, columns=self.item_list, dtype=float)
        for i, tid in enumerate(self.item_list):
            t_u_table_tmp = self.custer_txndeail_table[self.custer_txndeail_table['subclass'] == tid]
            for j, uid in enumerate(self.user_list):
                if uid in t_u_table_tmp["siebel_id"].unique():
                    count = len(t_u_table_tmp[t_u_table_tmp['siebel_id']==uid])
                    history_user_item_table.iloc[j, i] = count
        user_similarity = cosine_similarity(history_user_item_table.fillna(0).values)
        item_similarity = cosine_similarity(history_user_item_table.fillna(0).values.T)
        self.history_user_item_table = history_user_item_table
        self.user_similarity = pd.DataFrame(user_similarity,index=self.user_list,columns=self.user_list)
        self.item_similarity = pd.DataFrame(item_similarity,index=self.item_list,columns=self.item_list)
        self.__creat_user_similar_table()
    #给定人员找出相似度最高的top-n的人
    def get_similar_top_k(self,user_id='1-8I7A6C5',k=10):
        return list(self.user_similarity[user_id].sort_values(ascending=False).index[0:k])
    #建立用户(购买商品行为)最相似top-n表#存入数据库中
    def __creat_user_similar_table(self,n=10):
        user_similar_list = []
        # print(len(self.user_list))
        if n>len(self.user_list):
            n = len(self.user_list)
        for uid in self.user_list:
            u_list = self.user_similarity[uid].sort_values(ascending=False).index[0:n].tolist()
            user_similar_list.append(u_list)
        user_similar_table = pd.DataFrame(user_similar_list,index=self.user_list)
        self.user_similar_table = user_similar_table

    def get_seibel_id(self,member_id):
        sql = "select * from " + self.member_seibel + " where member_id= " + member_id
        data = pd.read_sql_query(sql, self.conn)
        if data['seibel_id'].values:
            return data['seibel_id'].values[0]
        else:
            logging.info('no this member_is to seibel_id')
            return None
    def get_seible_id_list(self,member_id_list):
        seibel_id_list = []
        for member_id in member_id_list:
            seibel_id_list.append(self.get_seibel_id(member_id))
        return seibel_id_list
    def get_brand_Q_score(self,cpns_list,brand_list):
        score = 0
        for brand_id in brand_list:
            for cpns_id in cpns_list:
                sql = "select * from coupon_brand_recommend_table  where brand_id= " + brand_id
                data = pd.read_sql_query(sql, self.conn)
                score += data[cpns_id].values[0]
        return score
    def get_cat_P_score(self,cpns_list,cat_list):
        score = 0
        for cat_id in cat_list:
            for cpns_id in cpns_list:
                sql = "select * from coupon_cat_recommend_table  where subclass= '" + cat_id + "'"
                data = pd.read_sql_query(sql, self.conn)
                score += data[cpns_id].values[0]
        return score
    #根据coupon计算出来的user_list,结合品牌、品类偏好进行评分,返回top_n用户
    def get_top_n_user(self,cpns_sim_list,user_dict, n):
        user_list = user_dict.keys()
        for member_id in user_list:
            seibel_id = self.get_seibel_id(member_id)
            if seibel_id is not None:
                sql = "select * from " + self.siebel_id_brand_id_q5 + " where seibel_id= " + seibel_id
                b_data = pd.read_sql_query(sql, self.conn)
                brand_list = b_data['concat_brandid'].values[0].split(',')
                user_dict[member_id]['brand_coupon_count'] = self.coupon_brand_count*self.get_brand_Q_score(cpns_sim_list,brand_list)
                user_dict[member_id]['total'] += user_dict[member_id]['brand_coupon_count']
                #------------------------------------------------------------------------------------
                sql = "select * from " + self.siebel_id_subclass_id_q5 + " where seibel_id= " + seibel_id
                c_data =  pd.read_sql_query(sql, self.conn)
                cat_list = c_data['subclass_id'].values[0].split(',')
                user_dict[member_id]['cat_coupon_count'] = self.coupon_cat_count*self.get_cat_P_score(cpns_sim_list,cat_list)
                user_dict[member_id]['total'] += user_dict[member_id]['cat_coupon_count']
        user_list = sorted(user_list.items(), key=lambda d: d[1]['total'], reverse=True)
        if n > len(user_list):
            n = len(user_list)
        return user_list[0:n]






