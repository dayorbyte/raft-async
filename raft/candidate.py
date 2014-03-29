import asyncio
import math
import sys
import traceback
from datetime import timedelta
from raft.constants import *

class CandidateMixin(object):
    @asyncio.coroutine
    def election_coroutine(self):
        ''' Background coroutine which handles the gathering and tallying of
            votes during a leadership election
        '''
        # Loop and check
        while True:
            # Check that we are a follower. If not, wait until we are
            yield from self.role.wait(CANDIDATE)
            yield from self._single_election()

    @asyncio.coroutine
    def _single_election(self):
        # Setup
        vote_count = 0
        term = self.current_term
        self.voted_for = self.candidate_id

        # Send requests to all peers
        futures = []
        for peer in self.peers:
            vote_f = peer.request_vote(term,
                self.candidate_id, self.log.last_index, self.log.term)
            futures.append(vote_f)

        # Get results as they come in
        for f in asyncio.as_completed(futures):
            vote_msg = yield from f
            count, stop = self._election_counting(vote_msg, term, vote_count)
            vote_count += count
            if stop:
                break
        else:
            # Got all results and didn't become leader. Sleep until
            # the end of the current term, then loop around and try
            # again
            diff = self.now() - self.current_term_end
            if diff > timedelta(seconds=0)
                yield asyncio.sleep(diff.total_seconds())

    def _election_counting(self, vote_msg, term, vote_count):
        ''' The algorithm for, given a message, deciding what to do next.
            This is the core of the election_coroutine, but is separate
            for ease of testing
        '''
        # The term changed while we were waiting for the response,
        # stop. This means that an appendEntries came in from the
        # new leader and we reverted to being a follower.
        if term != self.current_term:
            return (0, True)

        # Timed out on this election cycle. Stop!
        if self.now() > self.current_term_end:
            return (0, True)

        # Not a canidate anymore, stop!
        if self.role.state != CANDIDATE:
            return (0, True)

        # If the response has a higher term we can't become
        # the leader. Stop.
        if vote_msg['term'] > term:
            self.become_follower(vote_msg['term'])
            return (0, True)

        # If the vote wasn't granted, keep trying
        if not vote_msg['vote_granted']:
            return (0, False)

        # Update the vote and see if the election is won.
        if vote_count + 1 >= math.ceil(len(self.peers)/2):
            self.become_leader()
            return (1, True)

        # Add a vote, continue on
        return (1, False)

