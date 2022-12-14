import asyncio, time, inspect
from collections import deque

class Step():
    def __init__(self, workers=1, maxsize=None, func=None, handle_exceptions=False):
        self.workers = workers
        self.maxsize = maxsize
        self.func = func
        self.handle_exceptions = handle_exceptions
        self.next = None
        self.queue = asyncio.Queue(maxsize=maxsize or 0)
        self._time_samples = deque(maxlen=10)
        self._tasks = []

    def enable(self, recursive=True):
        if not self._tasks:
            for i in range(self.workers):
                self._tasks.append(asyncio.create_task(self._run_worker()))
        if recursive and self.next:
            self.next.enable(recursive)

    def disable(self, recursive=True):
        for i in self._tasks:
            i.cancel()
        self._tasks.clear()
        if recursive and self.next:
            self.next.disable(recursive)

    def clear(self, recursive=True):
        for _ in range(self.queue.qsize()):
            self.queue.get_nowait()
            self.queue.task_done()
        if recursive and self.next:
            self.next.clear(recursive)

    async def _run_worker(self):
        dq = self.DummyQueue()
        while True:
            item = await self.queue.get()
            outq = self.next.queue if self.next else dq
            
            # short-circuit if exception routing turned on
            if not self.handle_exceptions and isinstance(item, Exception):
                # if this is the last step and it does not handle exceptions, raise the exception
                if not self.next:
                    raise item
                else:
                    await outq.put(item)
                    self.queue.task_done()
                    continue
            
            func = self.func or self.run
            start = time.monotonic_ns()
            try:
                await func(item, outq)
            except Exception as e:
                if not self.handle_exceptions:
                    await outq.put(e)
                    self.queue.task_done()
                    continue
                else:
                    raise
            self._time_samples.append(time.monotonic_ns()-start)
            self.queue.task_done()

    async def run(self, item, outq):
        await outq.put(item)

    async def __call__(self, item):
        await self.queue.put(item)

    async def join(self):
        await self.queue.join()
        if self.next:
            await self.next.join()

    def execution_time(self, recursive=False):
        ''' Average execution time in seconds. '''
        average = sum(self._time_samples)/len(self._time_samples)
        average = average / 1000000000
        if recursive:
            self_ = {self: average}
            if self.next:
                next_ = self.next.execution_time(recursive=recursive)
                self_.update(next_)
            return self_
        else:
            return average

    def count(self, recursive=True):
        if recursive:
            self_ = {self: self.queue.qsize()}
            if self.next:
                next_ = self.next.count(recursive=recursive)
                self_.update(next_)
            return self_
        else:
            return self.queue.qsize()

    def steps(self):
        rightmost = self
        while True:
            yield rightmost
            if rightmost.next:
                rightmost = rightmost.next
            else:
                break

    def __rshift__(self, y):
        # this is likely O(n!)
        *_, last = self.steps()
        last.next = y
        return self

    class DummyQueue:
        async def put(self, *args, **kwargs):
            pass
        async def put_nowait(self, *args, **kwargs):
            pass