import pandas as pd
import numpy as np
import ComputeCosineSimilarity as ccs
import logging
import os.path as path
from sklearn.metrics.pairwise import cosine_similarity
from sklearn_pandas import DataFrameMapper
from sklearn.preprocessing import LabelBinarizer
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import FunctionTransformer

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO,filename='test.log')
class Coupon:
    def __init__(self, datadir="./data/",feature_list={'access_action_count':0.3,'use_action_count':0.7,'base':'base_item'},top_n = 100):
        self.datadir = datadir #文件路径
        self.coupon_scheme_table = None #优惠券定义表(用于抽取特征、找相似的券)
        self.coupon_access_uses_table = None #优惠券领用使用表(用于建立用户-优惠券关联表)
        self.user_list = None  #领取过优惠券的用户列表
        self.coupon_list = None #优惠券定义列表
        self.history_use_table = None #优惠券历史使用表
        self.coupon_encoder = None #优惠券特征
        self.recommend_top_n_user_table = None #推荐表topn
        self.f_recommend_user_table = None  # 特征关联表
        self.coupon_sim_table = None #优惠券相似度表（目前只提取了info相似度）
        self.feature_list = feature_list # 配置特征行为
        self.top_n = top_n  #top_n结果

    # 读取数据库
    def load_myqsl(self, conn, coupon_cust_access =None, coupon_cust_order = None,is_read_coupon_result = 0):
        logging.info('load coupon data from myqsl begin')
        sql_access = "select * from " + coupon_cust_access
        coupon_cust_access_table = pd.read_sql(sql_access, con=conn, columns=['cpns_id', 'member_id','num'])
        # coupon_cust_access_table = coupon_cust_access_table.iloc[0:300]
        # print(coupon_cust_access_table['cpns_id'].value_counts())
        # print(coupon_cust_access_table['member_id'].value_counts())
        sql_order = "select * from " + coupon_cust_order
        use_order_table = pd.read_sql(sql_order, con=conn, columns=['cpns_id', 'member_id', 'num'])
        # use_order_table = use_order_table.iloc[0:300]
        self.coupon_access_uses_table = use_order_table
        #-------------------
        # user_list = coupon_cust_access_table['member_id'].unique()
        # coupon_list = coupon_cust_access_table['cpns_id'].unique()
        # f_dataframe = pd.DataFrame(index=user_list, columns=coupon_list, dtype=float)
        # for i, cid in enumerate(coupon_list):
        #
        #     for j, uid in enumerate(user_list):
        #         sql = "select * from " + coupon_cust_access + " where cpns_id= " +cid + " and member_id= "+uid
        #         data = pd.read_sql_query(sql,conn)
        #         if data['num'].values:
        #             print(data['num'].values[0])
        #         # f_dataframe.iloc[j, i] =
        #-------------------
        logging.info('load coupon data from myqsl end')
        # self.__save_to_coupon_sim_table()
        if int(is_read_coupon_result) == 0:
            # self.__creat_recommend_table(coupon_cust_access_table,use_order_table)
            self.__creat_recommend_table_sql(coupon_cust_access_table,conn, coupon_cust_access=coupon_cust_access,
                                        coupon_cust_order=coupon_cust_order)
        else:
            # self.__read_recommend_table()
            self.__read_recommend_tabel_sql(conn)
        logging.info('creat recommend table done')
    def __read_recommend_table(self):
        logging.info('read coupon recommend begin')
        f_recommend_user_table = {}
        for feature_id, weight in self.feature_list.items():
            if feature_id == 'access_action_count' and weight != '0':
                f_recommend_user_table[feature_id] = pd.read_excel('coupon_user_access_base_coupon.xlsx',index_col=0)
            elif feature_id == 'use_action_count' and weight != '0':
                f_recommend_user_table[feature_id] = pd.read_excel('coupon_user_use_base_coupon.xlsx', index_col=0)
        self.f_recommend_user_table = f_recommend_user_table
        logging.info('read coupn recommend end')
    def __read_recommend_tabel_sql(self,conn):
        logging.info('read coupon recommend sql begin')
        f_recommend_user_table = {}
        for feature_id, weight in self.feature_list.items():
            if feature_id == 'access_action_count' and weight != '0':
                sql = "select * from " + "coupon_user_access_base_coupon"
                f_recommend_user_table[feature_id] =  pd.read_sql(sql, conn, index_col =['member_id'])
                # print(f_recommend_user_table[feature_id])
            elif feature_id == 'use_action_count' and weight != '0':
                sql = "select * from " + "coupon_user_use_base_coupon"
                f_recommend_user_table[feature_id] = pd.read_sql(sql, conn, index_col =['member_id'])
                # print(f_recommend_user_table[feature_id])
        self.f_recommend_user_table = f_recommend_user_table
        logging.info('read coupn recommend sql end')
    def __creat_recommend_table_sql(self,coupon_cust_access_table,conn,coupon_cust_access =None, coupon_cust_order = None):
        logging.info('creat coupon recommend sql begin')
        user_list = coupon_cust_access_table['member_id'].unique()
        coupon_list = coupon_cust_access_table['cpns_id'].unique()
        logging.info('user count %d,coupon count %d'%(len(user_list),len(coupon_list)))
        f_recommend_user_table = {}
        for feature_id, weight in self.feature_list.items():
            if feature_id == 'access_action_count' and weight != '0':
                f_dataframe = pd.DataFrame(index=user_list, columns=coupon_list, dtype=float)
                for i, cid in enumerate(coupon_list):
                    sql = "select * from " + coupon_cust_access + " where cpns_id= " + cid
                    c_table = pd.read_sql_query(sql,conn)
                    u_list = c_table['member_id'].unique()
                    for j, uid in enumerate(user_list):
                        if uid in u_list:
                            sql = "select * from " + coupon_cust_access + " where cpns_id= " + cid + " and member_id= " + uid
                            data = pd.read_sql_query(sql, conn)
                            f_dataframe.iloc[j, i] = data['num'].values[0]
                        if j%30000 ==0:
                            logging.info('%s coupon user process %d '%(cid,j))
                    if i%100==0:
                        logging.info('coupon process %d'%(i))
                ratings = f_dataframe.fillna(0.00001).values
                if self.feature_list['base'] == 'base_user':
                    # 计算用户相似度
                    user_similarity = cosine_similarity(ratings)
                    mean_user_rating = ratings.mean(axis=1)
                    ratings_diff = (ratings - mean_user_rating[:, np.newaxis])
                    predction = mean_user_rating[:, np.newaxis] + np.dot(user_similarity, ratings_diff) / np.array(
                        [np.abs(user_similarity).sum(axis=1)]).T
                    f_recommend_user_table[feature_id] = pd.DataFrame(predction * float(weight), index=user_list,
                                                                      columns=coupon_list, dtype=float)
                    # f_recommend_user_table[feature_id].rename_axis("member_id",inplace=True)
                    f_recommend_user_table[feature_id].reset_index(drop=False,inplace=True)
                    f_recommend_user_table[feature_id].rename(columns={'index': 'member_id'}, inplace=True)
                    f_recommend_user_table[feature_id].to_sql('coupon_user_access_base_user',conn,flavor='mysql',if_exists='replace',index=False,chunksize=10000)
                    logging.info('save coupon_user_access_base_user')
                else:
                    coupon_similarity = cosine_similarity(ratings.T)
                    predction = ratings.dot(coupon_similarity) / np.array([np.abs(coupon_similarity).sum(axis=1)])
                    f_recommend_user_table[feature_id] = pd.DataFrame(predction * float(weight), index=user_list,
                                                                      columns=coupon_list, dtype=float)
                    # f_recommend_user_table[feature_id].rename_axis("member_id",inplace=True)
                    f_recommend_user_table[feature_id].reset_index(drop=False,inplace=True)
                    f_recommend_user_table[feature_id].rename(columns={'index': 'member_id'}, inplace=True)
                    print(f_recommend_user_table[feature_id])
                    pd.io.sql.to_sql(f_recommend_user_table[feature_id],'coupon_user_access_base_coupon',conn,if_exists='replace',index=False,chunksize=10000)
                    logging.info('save coupon_user_access_base_coupon')
            elif feature_id == 'use_action_count' and weight != '0':
                f_dataframe = pd.DataFrame(index=user_list, columns=coupon_list, dtype=float)
                for i, cid in enumerate(coupon_list):
                    sql = "select * from " + coupon_cust_order + " where cpns_id= " + cid
                    c_table = pd.read_sql_query(sql, conn)
                    u_list = c_table['member_id'].unique()
                    for j, uid in enumerate(user_list):
                        if uid in u_list:
                            sql = "select * from " + coupon_cust_order + " where cpns_id= " + cid + " and member_id= " + uid
                            data = pd.read_sql_query(sql, conn)
                            f_dataframe.iloc[j, i] = data['num'].values[0]
                        if j%30000 ==0:
                            logging.info('%s coupon user process %d '%(cid,j))
                    if i%100==0:
                        logging.info('coupon process %d'%(i))
                ratings = f_dataframe.fillna(0.00001).values
                if self.feature_list['base'] == 'base_user':
                    # 计算用户相似度
                    user_similarity = cosine_similarity(ratings)
                    mean_user_rating = ratings.mean(axis=1)
                    ratings_diff = (ratings - mean_user_rating[:, np.newaxis])
                    predction = mean_user_rating[:, np.newaxis] + np.dot(user_similarity, ratings_diff) / np.array(
                        [np.abs(user_similarity).sum(axis=1)]).T
                    f_recommend_user_table[feature_id] = pd.DataFrame(predction * float(weight), index=user_list,
                                                                      columns=coupon_list, dtype=float)
                    # f_recommend_user_table[feature_id].rename_axis("member_id",inplace=True)
                    f_recommend_user_table[feature_id].reset_index(drop=False,inplace=True)
                    f_recommend_user_table[feature_id].rename(columns={'index': 'member_id'}, inplace=True)
                    f_recommend_user_table[feature_id].to_sql('coupon_user_use_base_user', conn,
                                                              if_exists='replace', index=False, chunksize=10000)
                    logging.info('save coupon_user_use_base_user')
                else:
                    coupon_similarity = cosine_similarity(ratings.T)
                    predction = ratings.dot(coupon_similarity) / np.array([np.abs(coupon_similarity).sum(axis=1)])
                    f_recommend_user_table[feature_id] = pd.DataFrame(predction * float(weight), index=user_list,
                                                                      columns=coupon_list, dtype=float)
                    # f_recommend_user_table[feature_id].rename_axis("member_id",inplace=True)
                    f_recommend_user_table[feature_id].reset_index(drop=False,inplace=True)
                    f_recommend_user_table[feature_id].rename(columns={'index': 'member_id'}, inplace=True)
                    print(f_recommend_user_table[feature_id])
                    pd.io.sql.to_sql(f_recommend_user_table[feature_id],'coupon_user_use_base_coupon', conn,index=False,
                                                              if_exists='replace', chunksize=10000)

                    logging.info('save coupon_user_use_base_coupon')
        self.f_recommend_user_table = f_recommend_user_table
        self.user_list = user_list
        self.coupon_list = coupon_list
        logging.info('creat coupon recommend sql end')
    # 优惠券历史领/用行为（建立特征相关表,mysql）
    def __creat_recommend_table(self,coupon_cust_access_table,use_order_table):
        logging.info('creat coupon recommend begin')
        user_list = coupon_cust_access_table['member_id'].unique()
        coupon_list = coupon_cust_access_table['cpns_id'].unique()
        f_recommend_user_table = {}
        for feature_id, weight in self.feature_list.items():
            if feature_id == 'access_action_count' and weight != '0':
                f_dataframe = pd.DataFrame(index=user_list, columns=coupon_list, dtype=float)
                for i, cid in enumerate(coupon_list):
                    c_table = coupon_cust_access_table[coupon_cust_access_table['cpns_id']==cid]
                    u_list = c_table['member_id'].unique()
                    for j, uid in enumerate(user_list):
                        if uid in u_list:
                            f_dataframe.iloc[j, i] = c_table[c_table['member_id'] == uid]['num'].values

                ratings = f_dataframe.fillna(0.00001).values
                if self.feature_list['base'] == 'base_user':
                    # 计算用户相似度
                    user_similarity = cosine_similarity(ratings)
                    mean_user_rating = ratings.mean(axis=1)
                    ratings_diff = (ratings - mean_user_rating[:, np.newaxis])
                    predction = mean_user_rating[:, np.newaxis] + np.dot(user_similarity, ratings_diff) / np.array(
                        [np.abs(user_similarity).sum(axis=1)]).T
                    f_recommend_user_table[feature_id] = pd.DataFrame(predction * float(weight), index=user_list,
                                                                      columns=coupon_list, dtype=float)
                    f_recommend_user_table[feature_id].to_excel('coupon_user_access_base_user.xlsx', sheet_name='coupon_user_access_base_user',
                                                   float_format="%.17f")
                    logging.info('save coupon_user_access_base_user')
                else:
                    coupon_similarity = cosine_similarity(ratings.T)
                    predction = ratings.dot(coupon_similarity) / np.array([np.abs(coupon_similarity).sum(axis=1)])
                    f_recommend_user_table[feature_id] = pd.DataFrame(predction * float(weight), index=user_list,
                                                                      columns=coupon_list, dtype=float)
                    f_recommend_user_table[feature_id].to_excel('coupon_user_access_base_coupon.xlsx',
                                                                sheet_name='coupon_user_access_base_coupon',
                                                                float_format="%.17f")
                    logging.info('save coupon_user_access_base_coupon')
            elif feature_id == 'use_action_count' and weight != '0':
                f_dataframe = pd.DataFrame(index=user_list, columns=coupon_list, dtype=float)
                for i, cid in enumerate(coupon_list):
                    c_table = use_order_table[use_order_table['cpns_id'] == cid]
                    u_list = c_table['member_id'].unique()
                    for j, uid in enumerate(user_list):
                        if uid in u_list:
                            f_dataframe.iloc[j, i] = c_table[c_table['member_id'] == uid]['num'].values

                ratings = f_dataframe.fillna(0.00001).values
                if self.feature_list['base'] == 'base_user':
                    # 计算用户相似度
                    user_similarity = cosine_similarity(ratings)
                    mean_user_rating = ratings.mean(axis=1)
                    ratings_diff = (ratings - mean_user_rating[:, np.newaxis])
                    predction = mean_user_rating[:, np.newaxis] + np.dot(user_similarity, ratings_diff) / np.array(
                        [np.abs(user_similarity).sum(axis=1)]).T
                    f_recommend_user_table[feature_id] = pd.DataFrame(predction * float(weight), index=user_list,
                                                                      columns=coupon_list, dtype=float)
                    f_recommend_user_table[feature_id].to_excel('coupon_user_use_base_user.xlsx',
                                                                sheet_name='coupon_user_use_base_user',
                                                                float_format="%.17f")
                    logging.info('save coupon_user_use_base_user')
                else:
                    coupon_similarity = cosine_similarity(ratings.T)
                    predction = ratings.dot(coupon_similarity) / np.array([np.abs(coupon_similarity).sum(axis=1)])
                    f_recommend_user_table[feature_id] = pd.DataFrame(predction * float(weight), index=user_list,
                                                                      columns=coupon_list, dtype=float)
                    f_recommend_user_table[feature_id].to_excel('coupon_user_use_base_coupon.xlsx',
                                                                sheet_name='coupon_user_use_base_coupon',
                                                                float_format="%.17f")
                    logging.info('save coupon_user_use_base_coupon')
        self.f_recommend_user_table = f_recommend_user_table
        self.user_list = user_list
        self.coupon_list = coupon_list
        logging.info('creat coupon recommend end')
    #读取tsv文件数据
    def load_tsv(self,scheme_filename,access_uses_filename):
        logging.info(path.join(self.datadir, scheme_filename))
        coupon_scheme_table = pd.read_csv(path.join(self.datadir, scheme_filename),nrows=3000,sep='\t',header=0)
        coupon_access_uses_table = pd.read_csv(path.join(self.datadir, access_uses_filename),nrows=3000,usecols=['cpns_id','member_id','order_id'],sep='\t',header=0)
        #过滤测试券以及用户
        # TODO
        user_list = coupon_access_uses_table['member_id'].unique()
        coupon_list = coupon_access_uses_table['cpns_id'].unique()
        self.coupon_scheme_table = coupon_scheme_table
        self.coupon_access_uses_table = coupon_access_uses_table
        self.user_list = user_list
        self.coupon_list = coupon_list
        # self.__save_to_coupon_sim_table()
        logging.info('load success.')
        # self. __extract_feature()
        self._save_to_recommend_table()
        logging.info('create history_use_tale success')
    #优惠券历史领/用行为（建立特征相关表）
    def _save_to_recommend_table(self):
        f_recommend_user_table = {}
        for feature_id ,weight in self.feature_list.items():
            if feature_id == 'access_action_count' and weight != '0':
                f_dataframe = pd.DataFrame(index=self.user_list,columns=self.coupon_list,dtype=float)
                for i, cid in enumerate(self.coupon_list):
                    c_u_table_tmp = self.coupon_access_uses_table[self.coupon_access_uses_table['cpns_id'] == cid]
                    c_u_table = c_u_table_tmp[c_u_table_tmp['order_id'].notnull() & c_u_table_tmp['order_id'] > 0]
                    for j, uid in enumerate(self.user_list):
                        if uid in c_u_table["member_id"].unique():
                            f_dataframe.iloc[j, i] = len(c_u_table_tmp[c_u_table_tmp['member_id'] == uid])

                ratings = f_dataframe.fillna(0.00001).values
                if self.feature_list['base'] == 'base_user':
                    # 计算用户相似度
                    user_similarity = cosine_similarity(ratings)
                    mean_user_rating = ratings.mean(axis=1)
                    ratings_diff = (ratings - mean_user_rating[:, np.newaxis])
                    predction = mean_user_rating[:, np.newaxis] + np.dot(user_similarity, ratings_diff) / np.array(
                         [np.abs(user_similarity).sum(axis=1)]).T
                    f_recommend_user_table[feature_id] = pd.DataFrame(predction * float(weight), index=self.user_list,
                                                                      columns=self.coupon_list, dtype=float)
                else:
                    coupon_similarity = cosine_similarity(ratings.T)
                    predction = ratings.dot(coupon_similarity) / np.array([np.abs(coupon_similarity).sum(axis=1)])
                    f_recommend_user_table[feature_id] = pd.DataFrame(predction * float(weight),index=self.user_list,columns=self.coupon_list,dtype=float)
                    f_recommend_user_table[feature_id].to_excel('testssfddss.xlsx', sheet_name='coupon_value_pnum',float_format="%.6f")

            elif feature_id == 'use_action_count' and weight != '0':
                f_dataframe = pd.DataFrame(index=self.user_list, columns=self.coupon_list, dtype=float)
                for i, cid in enumerate(self.coupon_list):
                    c_u_table_tmp = self.coupon_access_uses_table[self.coupon_access_uses_table['cpns_id'] == cid]
                    c_u_table = c_u_table_tmp[c_u_table_tmp['order_id'].notnull() & c_u_table_tmp['order_id'] > 0]
                    for j, uid in enumerate(self.user_list):
                        if uid in c_u_table["member_id"].unique():
                            f_dataframe.iloc[j, i] = len(c_u_table[c_u_table['member_id'] == uid])

                ratings = f_dataframe.fillna(0.00001).values
                if self.feature_list['base'] == 'base_user':
                    # 计算用户相似度
                    user_similarity = cosine_similarity(ratings)
                    mean_user_rating = ratings.mean(axis=1)
                    ratings_diff = (ratings - mean_user_rating[:, np.newaxis])
                    predction = mean_user_rating[:, np.newaxis] + np.dot(user_similarity, ratings_diff) / np.array([np.abs(user_similarity).sum(axis=1)]).T
                    print(predction)
                    f_recommend_user_table[feature_id] = pd.DataFrame(predction * float(weight), index=self.user_list,
                                                                      columns=self.coupon_list, dtype=float)
                else:
                    coupon_similarity = cosine_similarity(ratings.T)
                    predction = ratings.dot(coupon_similarity) / np.array([np.abs(coupon_similarity).sum(axis=1)])
                    f_recommend_user_table[feature_id] = pd.DataFrame(predction * float(weight),index=self.user_list,columns=self.coupon_list,dtype=float)

        self.f_recommend_user_table = f_recommend_user_table

    #给定一个user--生成评分组成（解释从哪种特征推荐得到）
    def get_feature_score(self,cpns_id,user_id):
        score = dict()
        total = 0
        for f_id , data in self.f_recommend_user_table.items():
            if f_id == 'use_action_count':
                f_id = 'feature_encoder_1'
            if f_id == 'access_action_count':
                f_id = 'feature_encoder_2'
            score[f_id] = data[cpns_id][user_id]
            total += data[cpns_id][user_id]
        score['total'] = total
        return score
    def __save_recommend_top_n_user_table(self , n= None):
        if n is None:
            n = int(self.top_n)
        # 为每种券保留top-n用户
        recommend_user_table = 0
        for key, data in self.f_recommend_user_table.items():
            recommend_user_table += data.values
            if self.user_list is None:
                self.user_list = data.index
            if self.coupon_list is None:
                self.coupon_list = data.columns
        recommend_user_table = pd.DataFrame(recommend_user_table, index=self.user_list, columns=self.coupon_list,
                                            dtype=float)
        user_list = []
        if n > len(self.user_list):
            n = len(self.user_list)
        for cid in self.coupon_list:
            u_list = recommend_user_table[cid].sort_values(ascending=False).index[0:n].tolist()
            user_list.append(u_list)
        recommend_top_n_user_table = pd.DataFrame(user_list, index=self.coupon_list)
        self.recommend_top_n_user_table = recommend_top_n_user_table
        self.top_n = n
    def get_top_n_user(self,cpns_list,n = None):
        if self.recommend_top_n_user_table is None:
            self.__save_recommend_top_n_user_table(n)
        if n>self.top_n:
            self.__save_recommend_top_n_user_table(n)
        user_list = {}
        for cpns_id in cpns_list:
            if cpns_id in self.recommend_top_n_user_table.index:
                for user_id in list(self.recommend_top_n_user_table.loc[cpns_id]):
                    user_list[str(user_id)] = self.get_feature_score(cpns_id,user_id)
        return user_list

    #优惠券特征抽取,暂时没用，等栗勇军他们完成info特征抽取
    def __extract_feature(self):
        mapper = DataFrameMapper([('ver', LabelBinarizer()),
                                  (['name'], LabelBinarizer()),
                                  (['create_account_id'], LabelBinarizer()),
                                  (['type'], FunctionTransformer(np.log1p)),
                                  (['department'], OneHotEncoder()),
                                  (['user_max_num'], OneHotEncoder()),
                                  (['max_num'], OneHotEncoder()),
                                  (['update_account_id'], LabelBinarizer()),
                                  (['amount'], OneHotEncoder()),
                                  (['discount'], OneHotEncoder()),
                                  (['max_discount_amount'], LabelBinarizer()),
                                  (['free_cond_amount'], OneHotEncoder()),
                                  (['device_limited'], LabelBinarizer()),
                                  (['novice_limited'], LabelBinarizer())])
        col_n = ['ver', 'name', 'create_account_id', 'type', 'department', 'user_max_num', 'max_num', 'update_account_id',
               'amount', 'discount', 'max_discount_amount', 'free_cond_amount', 'device_limited', 'novice_limited']
        coupon_select_cols = pd.DataFrame(self.coupon_scheme_table.fillna("nan"), columns=col_n)
        coupon_encoder = mapper.fit_transform(coupon_select_cols)
        self.coupon_encoder = coupon_encoder
    def __save_to_coupon_sim_table(self):
        Info = ccs.GetEventInfo()
        KeyWord = ccs.GetInfoElement()
        bi_num = ccs.InfoMatchKeyword(Info, KeyWord)
        ccs.ComputeSimilarity(bi_num)
        self.bi_num = bi_num
        logging.info('creat coupon info sim table done')
    #计算优惠券的相似度top-n
    def get_similar_top_n(self,features,n):
        #TODO
        coupon_list = []
        return coupon_list
        pass





