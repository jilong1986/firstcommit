import pandas as pd
from recommend_engine import Recommend_Engine
from concurrent.futures import ThreadPoolExecutor
# from tornado.concurrent import run_on_executor
# import tornado.httpserver
# import tornado.ioloop
# import tornado.options
# import tornado.web
# import tornado.gen
import json
import traceback
import logging
import re as reset
from flask import Flask,request
import json

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO,filename='test.log')
#创建引擎
re = Recommend_Engine()
# 给定一张新券
coupon_new = pd.DataFrame(
    {
        'id': 43,
        'cpns_id': ["22081094"],
        'ver': 3,
        'name': '20元伊利常温牛奶券',
        'source': 1,
        'create_account_id': "OA27182",
        'type': 1,
        'department': '002',
        'prefix': 'null',
        'user_max_num': 3,
        'max_num': 100000,
        'remind_days': '',
        'update_account_id': "OA27182",
        'del': 0,
        'effect': 1,
        'expire_time': "2018-04-15 23:59:59.000",
        'amount': 2000.0,
        'discount': 0.0,
        'max_discount_amount': '',
        'revalue': 0.0,
        'revalue_start_time': "2018-12-28 21:33:15.000",
        'revalue_end_time': "2018-12-28 21:33:15.000",
        'free_cond_amount': 0.0,
        'pic': 'null',
        'info': "单笔购伊利常温牛奶满100元使用一张，特价参与，仅限步步高梅溪湖超市使用会员付支付时使用",
        'create_time': "2018-03-16 15:49:34.000",
        'update_time': "2018-12-28 21:33:15.000",
        'device_limited': 0,
        'is_show': 0,
        'is_prior': '',
        'external_coupon_param': 'null',
        'is_record_sale': 0,
        'novice_limited': '',
    }
)
coupon_json = {
    "top_n": 100,
    "store_id": "120236",
    "shop_id": "4619",
    "coupon": [
        {
            'id': 43,
            'cpns_id': "22081093",
            'ver': 3,
            'name': '20元伊利常温牛奶券',
            'source': 1,
            'create_account_id': "OA27182",
            'type': 1,
            'department': '002',
            'prefix': 'null',
            'user_max_num': 3,
            'max_num': 100000,
            'remind_days': '',
            'update_account_id': "OA27182",
            'del': 0,
            'effect': 1,
            'expire_time': "2018-04-15 23:59:59.000",
            'amount': 2000.0,
            'discount': 0.0,
            'max_discount_amount': '',
            'revalue': 0.0,
            'revalue_start_time': "2018-12-28 21:33:15.000",
            'revalue_end_time': "2018-12-28 21:33:15.000",
            'free_cond_amount': 0.0,
            'pic': 'null',
            'info': "单笔购伊利常温牛奶满100元使用一张，特价参与，仅限步步高梅溪湖超市使用会员付支付时使用",
            'create_time': "2018-03-16 15:49:34.000",
            'update_time': "2018-12-28 21:33:15.000",
            'device_limited': 0,
            'is_show': 0,
            'is_prior': '',
            'external_coupon_param': 'null',
            'is_record_sale': 0,
            'novice_limited': ''
        }
    ]

}

# '''
# class MainHandler(tornado.web.RequestHandler):
#     executor = ThreadPoolExecutor(32)
#
#     @tornado.gen.coroutine
#     def get(self):
#         '''get接口'''
#         htmlStr = '''
#                     <!DOCTYPE HTML><html>
#                     <meta charset="utf-8">
#                     <head><title>Get page</title></head>
#                     <body>
#                     <form		action="/post"	method="post" >
#                     coupon:<br>
#                     <input type="textarea"      name ="coupon"     /><br>
#                     <input type="submit"	value="ok"	/>
#                     </form></body> </html>
#                 '''
#         self.write(htmlStr)
#
#     @tornado.web.asynchronous
#     @tornado.gen.coroutine
#     def post(self):
#         '''post接口， 获取参数'''
#         coupon = self.get_argument("coupon", None)
#         yield self.coreOperation(coupon)
#
#     @run_on_executor
#     def coreOperation(self, coupon):
#         '''主函数'''
#         try:
#             if coupon != '':
#                 coupon = reset.sub('\'', '\"', coupon)
#                 logging.info(coupon)
#                 coupon_data = json.loads(coupon)
#                 # print(coupon_data['top_n'])
#                 # print(coupon_data['store_id'])
#                 # print(coupon_data['shop_id'])
#                 # for coupon_i in coupon_data['coupon']:
#                 #     print(coupon_i['cpns_id'])
#                 #     print(coupon_i['info'])
#
#                 #继续添加json解析
#                 recommend_user_list, recall, precision = re.get_and_eval_top_n_user(coupon_data, 100)
#                 logging.info('recall:%f,precision:%f,top_n:%d' % (recall, precision, len(recommend_user_list)))
#                 logging.info(recommend_user_list)
#                 to_recommend_list = recommend_user_list.keys()
#                 result = recommend_user_list  # 可调用其他接口
#                 # result = coupon
#             else:
#                 result = json.dumps({'code': 211, 'result': 'wrong input coupon', })
#             self.write(result)
#         except Exception:
#             print('traceback.format_exc():\n%s' % traceback.format_exc())
#             result = json.dumps({'code': 503, 'result': coupon})
#             self.write(result)
#
# '''

app = Flask(__name__)

@app.route("/", methods=["post", "get"])
def my_test():
        try:
            coupon = request.json
            recommend_user_list, recall, precision = re.get_and_eval_top_n_user(coupon, 100)
            logging.info('recall:%f,precision:%f,top_n:%d' % (recall, precision, len(recommend_user_list)))
            logging.info(recommend_user_list)
            # to_recommend_list = recommend_user_list.keys()
            # result = recommend_user_list  # 可调用其他接口
            return json.dumps({"recommemd result":recommend_user_list})

            # if coupon != '':
            #     coupon = reset.sub('\'', '\"', coupon)
            #     logging.info(coupon)
            #     coupon_data = json.loads(coupon)
            #     # print(coupon_data['top_n'])
            #     # print(coupon_data['store_id'])
            #     # print(coupon_data['shop_id'])
            #     # for coupon_i in coupon_data['coupon']:
            #     #     print(coupon_i['cpns_id'])
            #     #     print(coupon_i['info'])
            #
            #     # 继续添加json解析
            #     # recommend_user_list, recall, precision = re.get_and_eval_top_n_user(coupon_data, 100)
            #     # logging.info('recall:%f,precision:%f,top_n:%d' % (recall, precision, len(recommend_user_list)))
            #     # logging.info(recommend_user_list)
            #     # to_recommend_list = recommend_user_list.keys()
            #     # result = recommend_user_list  # 可调用其他接口
            #     result = coupon_data
            # else:
            #     result = json.dumps({'code': 211, 'result': 'wrong input coupon', })
            # return json.dumps(result)
        except:
                return json.dumps({"data": "error"})


def main():
    # 创建服务
    app.run(host="0.0.0.0")
    '''
    tornado.options.parse_command_line()
    app = tornado.web.Application(handlers=[(r'/post', MainHandler)], autoreload=False, debug=False)
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(8832)
    tornado.ioloop.IOLoop.instance().start()
    '''

if __name__ == "__main__":
    main()