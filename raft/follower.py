from raft.constants import *
import asyncio
import traceback

class FollowerMixin(object):
    @asyncio.coroutine
    def election_timeout_coroutine(self):
        ''' Background coroutine which handles the gathering and tallying of
            votes during a leadership election
        '''
        # Loop and check
        while True:
            # Check that we are a follower. If not, wait until we are
            yield from self.role.wait(FOLLOWER)
            yield from self._single_timeout()

    @asyncio.coroutine
    def _single_timeout(self):
        ''' Background coroutine which handles the transition from follower
            to candidate
        '''
        # calculate the term end time at the start of sleeping
        current_term_end = self.current_term_end

        # calculate the difference between the current term end and
        # the
        now = self.now()
        if now < current_term_end:
          yield from asyncio.sleep((current_term_end - now).total_seconds())

        # If our term expired during sleep, become candidate
        if self.current_term_end <= self.now():
            self.become_candidate()
            return

        # If the term has not expired, that's great. It was probably
        # renewed. Let's start the loop over and sleep until the end of
        # the next term


