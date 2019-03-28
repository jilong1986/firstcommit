"""
        根据给出的会员ID列表，从f_member中抽取会员画像，将其插入到f_member_res
"""

import pymysql

# 从f_member中抽取单个会员画像，将其插入到f_member_res
def InsertAllinfoToPymysql(sie_id):
    conn = pymysql.connect(host='172.16.10.161', port=3306, user='root', password='root', db='tmp', charset='utf8')
    with conn.cursor() as cur:
        sql = "INSERT INTO f_member_res (siebel_id, gender, age, days, consumption_amount_12m, consumption_amount_6m, copn_receive_num, copn_use_num, copn_store_receive_num, copn_store_use_num, copn_store_use_num_12m, copn_receive_num_12m, copn_use_num_12m, copn_store_receive_num_12m, brand_q5, subclass_q5, tier, shop_id, birthday, reg_date) SELECT siebel_id, gender, age, days, consumption_amount_12m, consumption_amount_6m, copn_receive_num, copn_use_num, copn_store_receive_num, copn_store_use_num, copn_store_use_num_12m, copn_receive_num_12m, copn_use_num_12m, copn_store_receive_num_12m, brand_q5, subclass_q5, tier, shop_id, birthday, reg_date FROM f_member WHERE siebel_id = %s"

        cur.execute(sql, sie_id)

# 将所有的抽取出来的会员画像插入到f_member_res
def InsertClientPortraitToPymysql(siebel_id):
    for sie_id in siebel_id:
        InsertAllinfoToPymysql(sie_id)

# if __name__ == '__main__':
#     # siebel_id：推荐会员ID列表
#     siebel_id = ['1-1014B6N', '1-10525NG']
#     InsertClientPortraitToPymysql(siebel_id)

