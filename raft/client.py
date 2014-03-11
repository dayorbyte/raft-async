import sys
import tornado.ioloop
import tornado.iostream
import socket

def send_request():
    stream.write(b'''{"hello" : "world"}\n''')
    stream.read_until(b"\n", on_result)

def on_result(data):
    print data
    stream.close()
    tornado.ioloop.IOLoop.instance().stop()

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
stream = tornado.iostream.IOStream(s)
stream.connect(("localhost", int(sys.argv[1])), send_request)
tornado.ioloop.IOLoop.instance().start()