import sys
from datetime import timedelta
from tornado.ioloop import IOLoop
from tornado.tcpserver import TCPServer
from functools import wraps
TERM_MIN = 100
TERM_MAX = 200
FOLLOWER = 'FOLLOWER'
LEADER = 'LEADER'
CANDIDATE = 'CANDIDATE'

def lazy_stream(fun):
    @wraps(fun)
    @gen.coroutine
    def wrapper(self, *args, **kwargs):
        if self.stream is None:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
            self.stream = tornado.iostream.IOStream(s)
            yield self.stream.connect(("localhost", int(sys.argv[1])), send_request)
        res = yield fun(self, *args, **kwargs)
        raise gen.Return(res)

class LogEntry(object):
    def __init__(self, type, term, index, data):
        self.type = type
        self.term = term
        self.index = index
        self.data = data

class Peer(object):
    def __init__(self, host, port):
        self.stream = None
        self.host = host
        self.port = port

    @lazy_stream
    @gen.coroutine
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
    @gen.coroutine
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


class RaftServer(TCPServer):
    hosts = []
    # Persistent State
    current_term = None
    voted_for = None
    log = None

    # Volatile state
    commit_index = None
    last_applied = None

    # Other state
    term_end = None
    role = FOLLOWER


    # --------------------------------
    # Initialization
    # --------------------------------
    def __init__(self, host, port):
        self.candidate_id = '%s:%d' % (host, port)
        self.log = []
        # self.start_follower()

    # --------------------------------
    # Incoming Messages
    # --------------------------------
    def handle_stream(self, stream, address):
        msg = yield stream.read_until('\n')
        data = json.loads(msg)
        if msg['action'] == 'append_entries':
            res = self.append_entries(msg)
        self.write_json(stream, {
                            'result' : res,
                            'status' : 'OK',
                        })

    def write_json(self, stream, data):
        stream.write(json.dumps(data))

    # --------------------------------
    # Core Rules
    # --------------------------------

    # 1.    If commitIndex > lastApplied: increment lastApplied,
    #       apply log[lastApplied] to state machine (5.3)
    # 2.    If RPC request or response contains term T > currentTerm:
    #       set currentTerm = T, convert to follower (5.1)

    # --------------------------------
    # Follower Rules
    # --------------------------------
    # 1.    Respond to RPCs from candidates and leaders
    # 2.    If election timeout elapses without receiving AppendEntries
    #       RPC from current leader or granting vote to candidate:
    #       convert to candidate
    def append_entries(self, msg):
        # Leader/Candidate Reset after an election
        if self.role != FOLLOWER and msg['term'] > self.term:
            self.set_follower()
        # 1.
        if msg['term'] < self.term:
            return { 'term' : self.term, 'success' : False }
        # 2.
        if (msg['prev_index'] >= len(self.log)):
            return { 'term' : self.term, 'success' : False }
        # 3.
        if self.log[msg['prev_index']].term != msg['term']:


    # --------------------------------
    # Leader Rules
    # --------------------------------
    # 1.    Upon election: send initial empty AppendEntries RPCs (heartbeat)
    #       to each server; repeat during idle periods to prevent election timeouts (5.2)
    # 2.    If command received from client: append entry to local log,
    #       respond after entry applied to state machine (5.3)
    # 3.    If last log index >= nextIndex for a follower: send AppendEntries
    #       RPC with log entries starting at nextIndex
    #       a.  If successful: update nextIndex and matchIndex for
    #           follower (5.3)
    #       b.  If AppendEntries fails because of log inconsistency:
    #           decrement nextIndex and retry (5.3)
    # 4.    If there exists an N such that N > commitIndex, a majority of
    #       matchIndex[i] >= N, and log[N].term == currentTerm:
    #       set commitIndex = N (5.3, 5.4).
    def leader_routine(self):
        for p in self.peers:
            self.update_follower(peer)

    # --------------------------------
    # Candidate Rules
    # --------------------------------
    # 1.    On conversion to candidate, start election:
    #       a.  Increment currentTerm
    #       b.  Vote for self
    #       c.  Reset election timeout
    #       d.  Send RequestVote RPCs to all other servers
    # 6.    If votes received from majority of servers: become leader
    # 7.    If AppendEntries RPC received from new leader: convert to follower
    # 8.    If election timeout elapses: start new election



ports = range(0, int(sys.argv[1]))

for port in ports:
    try:
        server = RaftServer('localhost', 8000 + port)
        server.listen()
        print 'listening on: ' + str(8000 + port)
        server.hosts = [(localhost, (8000 + p)) for p in ports if p != port]
        break
    except:
        pass

IOLoop.current().start()



