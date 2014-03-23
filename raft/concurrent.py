
from asyncio import coroutine
from asyncio import Future

def success():
    f = Future()
    f.set_result(None)
    return f

class Condition(object):
    def __init__(self):
        self._waiters = []

    def wait(self):
        future = Future()
        self._waiters.append(future)
        return future

    def notify(self, n=1):
        if not self._waiters:
            return
        for i in range(0, n):
            self._waiters[i].set_result(None)
        self._waiters = self._waiters[n:]

    def notify_all(self):
        for f in self._waiters:
            f.set_result(None)


class StateMachine(object):
    def __init__(self, *states):
        self.current_state = None
        self.states = {}
        for s in states:
            self.states[s] = Condition()

    def wait(self, state):
        assert self.current_state is not None
        assert state in self.states
        if self.current_state == state:
            return success()
        return self.states[state].wait()

    def set_state(self, state):
        self.current_state = state
        self.states[state].notify_all()

