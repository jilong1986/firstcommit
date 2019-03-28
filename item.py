import pandas as pd
import logging
import os.path as path
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO,filename='test.log')

class Item:
    def __init__(self, datadir="./data/",top_n = None):
        self.datadir = datadir #文件路径
        self.custer_txndeail_table = None #消费明细表
        self.use_order_table = None #优惠券使用明细表
        self.coupon_list = None #优惠券列表
        self.subclass_list = None #商品列表
        self.top_n = None #topn商品
        self.coupon_subclass_recommend_table = None #优惠券-商品推荐表
        self.recommend_top_n_item_table = None #优惠券关联的n个商品
        self.coupon_brand_recommend_table = None #优惠券-品牌-评分表
        self.coupon_cat_recommend_table = None #优惠券-品类-评分表
        self.coupon_b_list = None #优惠券-品牌列表
        self.brand_c_list = None #品牌-优惠券列表
    #读取数据库
    def load_mysql(self,conn ,cpns_brand, cpns_cat,is_read_brand_cat_result = 0):
        logging.info('load brand cat - coupon from mysql begin')
        sql_brand = "select * from " + cpns_brand
        cpns_brand_table = pd.read_sql(sql_brand, con=conn ,columns=['cpns_id','brand_id', 'count(1)'])
        sql_cat = "select * from " + cpns_cat
        cpns_cat_table = pd.read_sql(sql_cat, con=conn ,columns=['cpns_id','subclass','count(1)'])

        logging.info('load brand cat - coupon from mysql end')
        logging.info('creat brand cat - coupon table begin')
        if int(is_read_brand_cat_result) == 0 :
            self. __creat_coupon_brand_subclass_score_table_sql(conn,cpns_brand,cpns_cat,cpns_brand_table ,cpns_cat_table)
        else:
            self.__read_brand_cat_score_table_sql(conn)
        logging.info('creat brand cat - coupon table end')
    def __read_brand_cat_score_table(self):
        logging.info('read  brand cat - coupon table begin')
        coupon_brand_recommend_table = pd.read_excel('coupon_brand_recommend_table.xlsx', index_col=0)
        coupon_cat_recommend_table = pd.read_excel('coupon_cat_recommend_table.xlsx', index_col=0)
        self.coupon_brand_recommend_table = coupon_brand_recommend_table
        self.coupon_cat_recommend_table = coupon_cat_recommend_table
        logging.info('read  brand cat - coupon table end')
    def __read_brand_cat_score_table_sql(self,conn):
        logging.info('read  brand cat - coupon table sql begin')
        sql = "select * from " + "coupon_brand_recommend_table"
        coupon_brand_recommend_table = pd.read_sql(sql, conn, index_col =['brand_id'])
        sql = "select * from " + "coupon_cat_recommend_table"
        coupon_cat_recommend_table = pd.read_sql(sql, conn, index_col =['subclass'])
        self.coupon_brand_recommend_table = coupon_brand_recommend_table
        self.coupon_cat_recommend_table = coupon_cat_recommend_table
        logging.info('read  brand cat - coupon table sql end')
    def __creat_coupon_brand_table_sql(self,conn,cpns_brand,cpns_brand_table):
        logging.info('creat brand - coupon from mysql begin')
        coupon_list = cpns_brand_table['cpns_id'].unique()
        brand_list = cpns_brand_table['brand_id'].unique()
        coupon_brand_table = pd.DataFrame(index=brand_list, columns=coupon_list, dtype=float)
        for i, bid in enumerate(brand_list):
            sql = "select * from " + cpns_brand + " where brand_id= " + bid
            b_cpns_table_tmp = pd.read_sql_query(sql, conn)
            c_list = b_cpns_table_tmp['cpns_id'].unique()
            for j, cid in enumerate(coupon_list):
                if cid in c_list:
                    sql = "select * from " + cpns_brand + " where brand_id= " + bid + " and cpns_id= " + cid
                    data = pd.read_sql_query(sql, conn)
                    if data['count(1)'].values:
                        coupon_brand_table.iloc[i, j] = np.log10(data['count(1)'].values[0]+1)
                if j % 300 == 0:
                    logging.info('%s coupon_brand cprocess %d ' % (cid, j))
            if i % 100 == 0:
                logging.info('brand process %d' % (i))
        ratings = coupon_brand_table.fillna(0.00001).values
        coupon_similarity = cosine_similarity(ratings.T)
        predction = ratings.dot(coupon_similarity) / np.array([np.abs(coupon_similarity).sum(axis=1)])
        coupon_brand_recommend_table = pd.DataFrame(predction, index=brand_list, columns=coupon_list, dtype=float)
        # coupon_brand_recommend_table.rename_axis("brand_id")
        coupon_brand_recommend_table.reset_index(drop=False, inplace=True)
        coupon_brand_recommend_table.rename(columns={'index': 'brand_id'}, inplace=True)
        coupon_brand_recommend_table.to_sql('coupon_brand_recommend_table', conn, if_exists='replace', index=False,
                                            chunksize=10000)
        self.coupon_brand_recommend_table = coupon_brand_recommend_table
        self.coupon_b_list = coupon_list
        self.brand_c_list = brand_list
        logging.info('creat brand - coupon from mysql end')
    def __creat_coupon_subclass_table_sql(self,conn,cpns_cat,cpns_cat_table):
        logging.info('creat cat - coupon from mysql begin')
        coupon_list = cpns_cat_table['cpns_id'].unique()
        cat_list = cpns_cat_table['subclass'].unique()
        coupon_cat_table = pd.DataFrame(index=cat_list, columns=coupon_list, dtype=float)
        for i, cid in enumerate(coupon_list):
            sql = "select * from " + cpns_cat + " where cpns_id= " + cid
            cat_cpns_table_tmp = pd.read_sql_query(sql, conn)
            cat_list_tmp = cat_cpns_table_tmp['subclass'].unique()
            for j, catid in enumerate(cat_list):
                if catid in cat_list_tmp:
                    sql = "select * from " + cpns_cat + " where subclass= '" + str(catid) + "' and cpns_id= " + cid
                    data = pd.read_sql_query(sql, conn)
                    # if data['count(1)'].values:
                    coupon_cat_table.iloc[j, i] = np.log10(data['count(1)'].values[0]+1)
                if j % 300 == 0:
                    logging.info('%s coupon-cat cat process %d ' % (cid, j))
            if i % 100 == 0:
                logging.info('cat process %d' % (i))
        ratings = coupon_cat_table.fillna(0.00001).values
        coupon_similarity = cosine_similarity(ratings.T)
        predction = ratings.dot(coupon_similarity) / np.array([np.abs(coupon_similarity).sum(axis=1)])
        coupon_cat_recommend_table = pd.DataFrame(predction, index=cat_list, columns=coupon_list, dtype=float)
        # coupon_cat_recommend_table.rename_axis("subclass")
        coupon_cat_recommend_table.reset_index(drop=False, inplace=True)
        coupon_cat_recommend_table.rename(columns={'index': 'subclass'}, inplace=True)
        coupon_cat_recommend_table.to_sql('coupon_cat_recommend_table', conn, if_exists='replace',
                                          index=False, chunksize=10000)
        self.coupon_cat_recommend_table = coupon_cat_recommend_table
        self.coupon_cat_list = coupon_list
        self.cat_coupon_list = cat_list
        logging.info('creat cat - coupon from mysql end')
    def __creat_coupon_brand_subclass_score_table_sql(self,conn,cpns_brand,cpns_cat,cpns_brand_table ,cpns_cat_table):
        self.__creat_coupon_brand_table_sql(conn, cpns_brand, cpns_brand_table)
        self.__creat_coupon_subclass_table_sql(conn,cpns_cat,cpns_cat_table)

    #建立优惠券-品牌、优惠券-品类相关表
    def __creat_coupon_brand_subclass_score_table(self,cpns_brand_table ,cpns_cat_table):
        logging.info('creat brand - coupon from mysql begin')
        coupon_list = cpns_brand_table['cpns_id'].unique()
        brand_list = cpns_brand_table['brand_id'].unique()
        coupon_brand_table = pd.DataFrame(index=brand_list, columns=coupon_list, dtype=float)
        for i, bid in enumerate(brand_list):
            b_cpns_table_tmp = cpns_brand_table[cpns_brand_table['brand_id'] == bid]
            c_list = b_cpns_table_tmp['cpns_id'].unique()
            for j, cid in enumerate(coupon_list):
                if cid in c_list:
                    coupon_brand_table.iloc[i, j] = b_cpns_table_tmp[b_cpns_table_tmp['cpns_id']==cid]['count(1)'].values
        ratings = coupon_brand_table.fillna(0.00001).values
        coupon_similarity = cosine_similarity(ratings.T)
        predction = ratings.dot(coupon_similarity) / np.array([np.abs(coupon_similarity).sum(axis=1)])
        coupon_brand_recommend_table = pd.DataFrame(predction , index=brand_list, columns=coupon_list, dtype=float)
        coupon_brand_recommend_table.to_excel('coupon_brand_recommend_table.xlsx',
                                                    sheet_name='coupon_brand_recommend_table',
                                                    float_format="%.17f")
        self.coupon_brand_recommend_table = coupon_brand_recommend_table
        self.coupon_b_list = coupon_list
        self.brand_c_list = brand_list
        logging.info('creat brand - coupon from mysql end')
        #------------------------------------------------------------------------------------------------------------------
        logging.info('creat cat - coupon from mysql begin')
        coupon_list = cpns_cat_table['cpns_id'].unique()
        cat_list = cpns_cat_table['subclass'].unique()
        coupon_cat_table = pd.DataFrame(index=cat_list, columns=coupon_list, dtype=float)
        for i, catid in enumerate(cat_list):
            cat_cpns_table_tmp = cpns_cat_table[cpns_cat_table['subclass'] == catid]
            c_list = cat_cpns_table_tmp['cpns_id'].unique()
            for j, cid in enumerate(coupon_list):
                if cid in c_list:
                    coupon_brand_table.iloc[i, j] = cat_cpns_table_tmp[cat_cpns_table_tmp['cpns_id'] == cid]['count(1)'].values
        ratings = coupon_cat_table.fillna(0.00001).values
        coupon_similarity = cosine_similarity(ratings.T)
        predction = ratings.dot(coupon_similarity) / np.array([np.abs(coupon_similarity).sum(axis=1)])
        coupon_cat_recommend_table = pd.DataFrame(predction, index=cat_list, columns=coupon_list, dtype=float)
        coupon_cat_recommend_table.to_excel('coupon_cat_recommend_table.xlsx',
                                              sheet_name='coupon_cat_recommend_table',
                                              float_format="%.17f")
        self.coupon_cat_recommend_table = coupon_cat_recommend_table
        self.coupon_cat_list = coupon_list
        self.cat_coupon_list = cat_list
        logging.info('creat cat - coupon from mysql end')
    #给定cpns_id求出最相关的top_n个品牌
    def get_top_n_brand_score(self,cpns_id,n=10):
        return list(self.coupon_brand_recommend_table[cpns_id].sort_values(ascending=False).index[0:n])
    #给定cpns_id求出最相关的top_n个品类
    def get_top_n_cat_score(self,cpns_id,n=10):
        return list(self.coupon_cat_recommend_table[cpns_id].sort_values(ascending=False).index[0:n])
    #读取tsv文件数据
    def load_tsv(self,use_order_filename, custer_txndeail_filename01,custer_txndeail_filename02):
        custer_txndeail_table01 = pd.read_csv(path.join(self.datadir, custer_txndeail_filename01),
                                              usecols=['tran_seq_no','item', 'subclass'], sep='\t', header=0)
        custer_txndeail_table02 = pd.read_csv(path.join(self.datadir, custer_txndeail_filename02),
                                              usecols=['tran_seq_no','item', 'subclass'], sep='\t', header=0)
        custer_txndeail_table = pd.concat([custer_txndeail_table01, custer_txndeail_table02])
        use_order_table = pd.read_csv(path.join(self.datadir, use_order_filename), nrows=600,usecols=['cpns_id','tran_seq_no','bn'], sep='\t', header=0)
        self.custer_txndeail_table = custer_txndeail_table
        self.use_order_table = use_order_table
    #建立商品 - 卡券关联表
    def _save_to_recommend_table(self,feature_list = {'base':'base_coupon'}):
        tran_seq_no_list = self.use_order_table['tran_seq_no'].unique()
        coupon_item_data = {
            "cpns_id": [],
            "subclass": []
        }
        for tsn in tran_seq_no_list:
            use_order_tsn = self.use_order_table[self.use_order_table['tran_seq_no'] == tsn]
            custer_txndeail_tsn = self.custer_txndeail_table[self.custer_txndeail_table['tran_seq_no'] == tsn]
            cpns_id = use_order_tsn['cpns_id'].unique()[0]
            for bn in use_order_tsn['bn'].unique():
                subclass = custer_txndeail_tsn[custer_txndeail_tsn['item'] == bn]['subclass'].values[0]
                coupon_item_data['cpns_id'].append(cpns_id)
                coupon_item_data['subclass'].append(subclass)
        coupon_item_table = pd.DataFrame(coupon_item_data, columns=['cpns_id', 'subclass'])
        coupon_list = coupon_item_table['cpns_id'].unique()
        subclass_list = coupon_item_table['subclass'].unique()
        self.coupon_list = coupon_list
        self.subclass_list = subclass_list
        #构建关联表
        coupon_subclass_table = pd.DataFrame(index=subclass_list,columns=coupon_list,dtype=float)
        for i, sid in enumerate(subclass_list):
            s_c_table_tmp = coupon_item_table[coupon_item_table['subclass'] == sid]
            for j, cid in enumerate(coupon_list):
                if cid in s_c_table_tmp["cpns_id"].unique():
                    coupon_subclass_table.iloc[j, i] = len(s_c_table_tmp[s_c_table_tmp['cpns_id'] == cid])
        ratings = coupon_subclass_table.fillna(0.00001).values
        if feature_list['base'] == 'base_item':
            # 计算用户相似度
            user_similarity = cosine_similarity(ratings)
            mean_user_rating = ratings.mean(axis=1)
            ratings_diff = (ratings - mean_user_rating[:, np.newaxis])
            predction = mean_user_rating[:, np.newaxis] + np.dot(user_similarity, ratings_diff) / np.array(
                [np.abs(user_similarity).sum(axis=1)]).T
            coupon_subclass_recommend_table = pd.DataFrame(predction , index=subclass_list,columns=coupon_list, dtype=float)
        else:
            coupon_similarity = cosine_similarity(ratings.T)
            predction = ratings.dot(coupon_similarity) / np.array([np.abs(coupon_similarity).sum(axis=1)])
            coupon_subclass_recommend_table = pd.DataFrame(predction ,index=subclass_list,columns=coupon_list, dtype=float)
        self.coupon_subclass_recommend_table = coupon_subclass_recommend_table
    #为每个卡券建立N个最相关的商品
    def __save_recommend_top_n_item_table(self , n= None):
        if n is None:
            n = int(self.top_n)
        # 为每种券保留top-n商品
        item_list = []
        if n > len(self.subclass_list):
            n = len(self.subclass_list)
        for cid in self.coupon_list:
            u_list = self.coupon_subclass_recommend_table[cid].sort_values(ascending=False).index[0:n].tolist()
            item_list.append(u_list)
        recommend_top_n_item_table = pd.DataFrame(item_list, index=self.coupon_list)
        self.recommend_top_n_item_table = recommend_top_n_item_table
    #提取给定卡券的N个相关商品
    def get_top_n_item(self,cpns_list,n = None):
        if self.recommend_top_n_item_table is None:
            self.__save_recommend_top_n_item_table(self, n)
        item_list = {}
        for cpns_id in cpns_list:
            for item_id in list(self.recommend_top_n_item_table.loc[cpns_id]):
                item_list[item_id] = self.get_feature_score(cpns_id,item_id)
        return item_list
    #提取得分
    def get_feature_score(self,cpns_id,item_id):
        return self.recommend_top_n_item_table[cpns_id][item_id]