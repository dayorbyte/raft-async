
def lazy_stream(fun):
    @wraps(fun)
    @coroutine
    def wrapper(self, *args, **kwargs):
        if self.stream is None:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
            self.stream = tornado.iostream.IOStream(s)
            yield self.stream.connect(("localhost", int(sys.argv[1])), send_request)
        res = yield fun(self, *args, **kwargs)
        raise gen.Return(res)

class Peer(object):
    def __init__(self, host, port):
        self.stream = None
        self.host = host
        self.port = port
        self.id = '{}:{}'.format(host, port)

    @lazy_stream
    @coroutine
    def request_vote(self, term, candidate_id, log_index, log_term):
        msg = {
            'action' : 'request_vote',
            'arguments' : { 'term' : int(term),
                            'candidate_id' : candidate_id,
                            'log_index'    : log_index,
                            'log_term'     : log_term,
                          }
        }
        yield self.stream.write(json.dumps(msg) + '\n')
        resp = yield self.stream.read_until(b"\n")
        response = json.loads(resp)
        raise gen.Return(response)

    @lazy_stream
    @coroutine
    def append_entries(self, term, leader_id, prev_index, prev_term,
                       entries, leader_commit_index):
        msg = {
            'action' : 'append_entries',
            'arguments' : {
                'term' : int(term),
                'leader_id' : leader_id,
                'prev_index' : prev_index,
                'prev_term' : prev_term,
                'entries' : entries,
                'leader_commit_index' : leader_commit_index,
            }
        }
        yield self.stream.write(json.dumps(msg) + '\n')
        resp = yield self.stream.read_until(b"\n")
        raise gen.Return(response)