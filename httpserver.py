# -*- coding:utf-8 -*-
# -*- created by: mo -*-
 
from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.gen
import json
import traceback

class MainHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(32)
    @tornado.gen.coroutine
    def get(self):
        '''get接口'''
        htmlStr = '''
                    <!DOCTYPE HTML><html>
                    <meta charset="utf-8">
                    <head><title>Get page</title></head>
                    <body>
                    <form		action="/post"	method="post" >
                    coupon:<br>
                    <input type="textarea"      name ="coupon"     /><br>
                    <input type="submit"	value="ok"	/>
                    </form></body> </html>
                '''
        self.write(htmlStr)
 
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def post(self):
        '''post接口， 获取参数'''
        coupon = self.get_argument("coupon", None)
       
        yield self.coreOperation(coupon)
 
    @run_on_executor
    def coreOperation(self, coupon):
        '''主函数'''
        try:
            if  coupon != '' :
                #json.load(coupon)
                result = coupon  #可调用其他接口
            else:
                result = json.dumps({'code': 211, 'result': 'wrong input coupon', })
            self.write(result)
        except Exception:
            print ('traceback.format_exc():\n%s' % traceback.format_exc())
            result = json.dumps({'code': 503,'result': coupon })
            self.write(result)
 
 
# if __name__ == "__main__":
#     #tornado.ioloop.IOLoop.instance().stop()
#     tornado.options.parse_command_line()
#     app = tornado.web.Application(handlers=[(r'/post', MainHandler)], autoreload=False, debug=False)
#     http_server = tornado.httpserver.HTTPServer(app)
#     http_server.listen(8832)
#     tornado.ioloop.IOLoop.instance().start()
