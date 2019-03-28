import requests
import json
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

url = 'http://192.168.199.114:5000/'
data = requests.post(url=url,json=coupon_json)
print(data.text)
