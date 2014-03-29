from raft.constants import *
import math
import asyncio
import traceback

class LeaderMixin(object):
    @property
    def is_leader(self):
        return self.role == LEADER

    @asyncio.coroutine
    def leader_coroutine(self):
        # Loop and check
        while True:
            # Check that we are a follower. If not, wait until we are
            yield from self.role.wait(LEADER)
            # Reset volatile leader state
            self.next_indexes = {}
            self.match_indexes = {}
            yield from self._single_leader_term()

    @asyncio.coroutine
    def _single_leader_term(self):
        for p in self.peers:
            # TODO: The paper calls for + 1, but it seems like if I use
            #       last_index I guarantee syncing is caught up before
            #       the last_index + 1 entry comes in.
            self.next_indexes[p.id] = self.log.last_index + 1
            self.match_indexes[p.id] = 0
            self.get_event_loop().call_soon(self._heartbeat, p)
            self.get_event_loop().call_soon(self._update_peer, p)

    @asyncio.coroutine
    def _heartbeat(self, peer):
        # Hearbeat every 50ms
        while True:
            if not self.is_leader:
                break
            yield from peer.send_heartbeat()
            if not self.is_leader:
                break
            yield from asyncio.sleep(HEARTBEAT_INTERVAL)

    @asyncio.coroutine
    def _update_peer(self, peer):
        # Start from the end, move backwards as needed
        while True:
            yield from self._update_peer_once(peer)
            if not self.is_leader:
                break

    @asyncio.coroutine
    def _update_peer_once(self, peer, try_index):
        # If we're out of entries to update, wait for the next one
        if self.next_indexes[peer.id] > self.log.last_index:
            yield from self.log.wait_for_new_entry.wait()
            if not self.is_leader:
                return

        # Set up RPC
        prev_index = None
        prev_term = None
        if self.next_indexes[peer.id] > 0:
            prev_index = self.next_indexes[peer.id] - 1
            prev_term = self.log.term_for_index(prev_index)
        entries = self.log.range(self.next_indexes[peer.id])
        # Send RPC
        res = yield from peer.append_entries(
            self.term,
            self.candidate_id,
            prev_index,
            prev_term,
            entries,
            self.commit_index)

        # If no longer leader, stop
        if not self.is_leader:
            return

        # If the peer's term is higher than ours, convert to follower and stop
        if res['current_term'] > self.current_term:
            self.become_follower(res['current_term'])
            return

        # If successful
        if res['success']:
            # Set matches to next index (which we just sent)
            self.match_indexes[peer.id] = self.next_indexes[peer.id]
            self.update_commit_index()
            # Update next index to be the following index
            self.next_indexes[peer.id] = self.next_indexes[peer.id] + 1
        else:
            self.next_indexes[peer.id] = self.next_indexes[peer.id] - 1

    def update_commit_index(self):
        indexes = sorted(self.match_indexes).values()
        pivotal_index = math.ceil(len(self.match_indexes)/2)
        median = indexes[pivotal_index]
        if median > self.commit_index:
            self.commit_index = median



