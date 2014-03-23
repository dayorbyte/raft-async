import asyncio
import random
from asyncio import coroutine
from raft.concurrent import StateMachine
import sys
import traceback
from functools import wraps
from raft.log import RaftLog
from datetime import datetime, timedelta
TERM_MIN = 100
TERM_MAX = 200
FOLLOWER = 'FOLLOWER'
LEADER = 'LEADER'
CANDIDATE = 'CANDIDATE'

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

class RaftServer(object):

    def __init__(self, host, port, log,
                 min_timeout=200, max_timeout=300):
        self.candidate_id = '%s:%d' % (host, port)
        self.log = log
        self.hosts = []

        # Persistent State
        self.current_term = 0
        self.voted_for = None

        # Volatile state
        self.commit_index = None
        self.last_applied = None

        # Other state
        self.term_end = None
        self.role = StateMachine(FOLLOWER, LEADER, CANDIDATE)
        self.roll.set_state(FOLLOWER)
        self.leader_id = None

    def process_append_entries(self, msg):
        # Set the leader to be the leader_id from the sender
        self.leader_id = msg['leader_id']

        # Leader/Candidate Reset after an election
        if self.role != FOLLOWER and msg['term'] > self.term:
            self.set_follower()
        self.log.append_entries(msg)

    def process_message(self, msg):
        action = msg.get('action')
        if action == 'append_entries':
            return self.process_append_entries(msg)
        elif action == 'request_vote':
            return self.process_request_vote(msg)
        return { 'status' : 'ERROR',
                 'msg'    : 'Unknown action type: ' + str(action)
               }
    @property
    def rand_timeout(self):
        return random.randint(self.min_timeout, self.max_timeout)

    @coroutine
    def election_coroutine(self):
        # Loop and check
        while True:
            # Check that we are a follower. If not, wait until we are
            yield from self.role.wait(FOLLOWER)


    @coroutine
    def election_timeout_coroutine(self):
        # Set initial term end
        if self.term_end is None:
            self.term_end = datetime.utcnow() + timedelta(milliseconds=self.rand_timeout)

        # Loop and check
        while True:
            # Check that we are a follower. If not, wait until we are
            yield from self.role.wait(FOLLOWER)

            # calculate the term end time at the start of sleeping
            current_term_end = self.term_end

            # calculate the difference between the current term end and
            # the
            end_ms = (current_term_end - datetime.utcnow()).total_seconds()
            yield asyncio.sleep(end_ms)

            # If our term expired during sleep, become candidate
            if self.term_end <= datetime.utcnow():
                self.become_candidate()
                continue

            # If the term has not expired, that's great. It was probably
            # renewed. Let's start the loop over and sleep until the end of
            # the next term



class RaftHandler(asyncio.Protocol):
    ''' Handles the networking portion of incoming connections
    '''
    def __init__(self, server):
        self.server = server

    # --------------------------------
    # Incoming Messages
    # --------------------------------
    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        print('connection from {}'.format(peername))
        self.transport = transport

    def data_received(self, data):
        data = data.decode()
        print('data received: {}'.format(len(data)))
        response = self.server.process_message(json.loads(data))
        self.write_json(self.transport, response)
        self.transport.write(data)
        self.transport.close()

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

loop = asyncio.get_event_loop()
log = RaftLog()

for port in ports:
    try:
        def RaftProtocol():
            return RaftHandler('localhost', 8000 + port, log)
        coro = loop.create_server(RaftProtocol, 'localhost', 8000 + port)
        server = loop.run_until_complete(coro)
        # server.listen()
        print('listening on: ' + str(8000 + port))
        server.hosts = [('localhost', (8000 + p)) for p in ports if p != port]
        break
    except OSError:
        pass
    except Exception as e:
        traceback.print_exc()

try:
    loop.run_forever()
except KeyboardInterrupt:
    print("exit")
finally:
    server.close()
    loop.close()

# IOLoop.current().start()



