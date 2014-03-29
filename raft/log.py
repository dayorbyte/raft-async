from raft.flow import

class LogEntry(object):
    def __init__(self, *, type, term, index, data):
        self.type = type
        self.term = term
        self.index = index
        self.data = data

class RaftLog(object):
    # --------------------------------
    # Initialization
    # --------------------------------
    def __init__(self):
        self.log = []
        self.wait_for_new_entry = Condition()

    def range(self, start, end=None):
        if end is None:
            return self.log[start:]
        return self.log[start:end]

    def term_for_index(self, index):
        assert index > 0
        return self[index].term
    @property
    def last_index(self):
        return len(self.log) - 1

    @property
    def term(self):
        if not log:
            return 0
        return self.log[-1].term

    def append_entries(self, msg):
        # 1. Reply false if term < currentTerm
        if msg['term'] < self.term:
            return { 'term' : self.term, 'success' : False }

        # NOTE: At this point we know that we aren't ahead of the leader
        #
        # 2. Reply false if log doesn’t contain an entry at prevLogIndex
        #    whose term matches prevLogTerm
        if (msg['prev_index'] >= len(self.log)):
            return { 'term' : self.term, 'success' : False }
        if self.log[msg['prev_index']].term != msg['prev_term']:
            self.log = self.log[:msg['prev_index']]
            return { 'term' : self.term, 'success' : False }

        # NOTE: At this point we know that the previous index matches
        #
        # 3. If an existing entry conflicts with a new one (same index but
        #    different terms), delete the existing entry and all that
        #    follow it
        if not msg['entries']:
            self.log = self.log[:msg['prev_index']+1]
        else:
            for entry in msg['entries']:
                index = entry['index']
                # If index > the log entries we have it is OK
                if index >= len(self.log):
                    continue
                # if entry is < log index, make sure the term is the same
                if entry['term'] != self.log[index].term:
                    self.log = self.log[:index]

        # 4. Append any new entries not already in the log
        for entry in msg['entries']:
            le = LogEntry(entry['type'],
                          entry['term'],
                          entry['index'],
                          entry['data'])
            self.append_one(le)

        # 5. If leaderCommit > commitIndex,
        #       set commitIndex = min(leaderCommit, last log index)
        if (self.commit_index is None or
                msg['commit_index'] > self.commit_index):
            new_index = min(len(self.log), msg['commit_index'])
            assert self.commit_index <= new_index
            self.commit_index = new_index
        return { 'term' : self.term, }

    def append_one(self, log_entry):
        self.log.append(le)
        # Notify waiters and reset the new entry condition variable
        self.wait_for_new_entry.notify_all()
        self.wait_for_new_entry = Condition()

