import asyncio
import math
import random
from asyncio import coroutine
from raft.flow import StateMachine
import sys
import traceback
from functools import wraps
from raft.log import RaftLog
from datetime import datetime, timedelta
from raft.constants import *
from raft.follower import FollowerMixin
from raft.leader import LeaderMixin
from raft.candidate import CandidateMixin

class RaftServer(FollowerMixin, CandidateMixin, LeaderMixin):
    def __init__(self, host, port, log, peers, min_timeout=200, max_timeout=300,
                 now_fun=datetime.utcnow):
        self.now = now_fun
        self.candidate_id = '%s:%d' % (host, port)
        self.peers = peers
        self.log = log
        self.hosts = []
        self.min_timeout = min_timeout
        self.max_timeout = max_timeout

        # Persistent State
        self.current_term = 0
        self.voted_for = None

        # Volatile state
        self.commit_index = None
        self.last_applied = None

        # Leader state
        self.next_indexes = {}
        self.match_indexes = {}

        # Other state
        self.current_term_end = None
        self.reset_term_end()

        self.role = StateMachine(FOLLOWER, LEADER, CANDIDATE)
        self.roll.set_state(FOLLOWER)
        self.leader_id = None

    def start(self, loop):
        loop.call_soon(self.election_coroutine)
        loop.call_soon(self.election_timeout_coroutine)
        loop.call_soon(self.leader_coroutine)

    # -------------------------------------------------------------------------
    #
    #   Incoming Request Coroutines
    #
    # -------------------------------------------------------------------------
    def process_request_vote(self, msg):
        term = msg['term']
        candidate_id = msg['candidate_id']
        last_log_index = msg['last_log_index']
        last_log_term = msg['last_log_term']
        result_fun = lambda res : dict(term=self.current_term,
                                       vote_granted=res)
        # Term is too low, reject
        if term < self.current_term:
            return result_fun(False)

        # Already chose a candidate, reject
        if self.voted_for is not None and self.voted_for != candidate_id:
            return result_fun(False)

        # Log term is too old, reject
        if last_log_term < self.log.term:
            return result_fun(False)

        # Not enough entries in the log, reject
        if last_log_index < self.log.index:
            return result_fun(False)

        # Passed all the tests, allocate vote to candidate!
        self.voted_for = candidate_id
        return result_fun(True)

    def process_append_entries(self, msg):
        # Set the leader to be the leader_id from the sender
        self.leader_id = msg['leader_id']

        # Leader/Candidate Reset after an election
        if self.role.state != FOLLOWER and msg['term'] > self.current_term:
            self.become_follower(msg['term'])
        return self.log.append_entries(msg)

    def process_message(self, msg):
        action = msg.get('action')
        if action == 'append_entries':
            return self.process_append_entries(msg)
        elif action == 'request_vote':
            return self.process_request_vote(msg)
        return dict(status='ERROR',
                    msg='Unknown action type: {}'.format(str(action)))

    # -------------------------------------------------------------------------
    #
    #   Helper Methods
    #
    # -------------------------------------------------------------------------
    @property
    def rand_timeout(self):
        return random.randint(self.min_timeout, self.max_timeout)

    def become_candidate(self):
        self.role.set_state(CANDIDATE)
        self.set_term(self.current_term + 1)
        self.reset_term_end()

    def become_follower(self, term):
        self.role.set_state(FOLLOWER)
        self.set_term(term)
        self.reset_term_end()

    def become_leader(self):
        self.role.set_state(LEADER)

    def reset_term_end(self):
        now = self.now()
        delta = timedelta(milliseconds=self.rand_timeout)
        self.current_term_end = now + delta

    def set_term(self, term):
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None

class RaftHandler(asyncio.Protocol):
    ''' Handles incoming network requests to the server
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

