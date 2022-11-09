import asyncio, time, inspect
from collections import deque
from abc import ABC, abstractmethod

class Step(ABC):
    def __init__(self, workers=1, maxsize=None, func=None):
        self.workers = workers
        self.maxsize = maxsize
        self.func = func
        self.next = None
        self.queue = asyncio.Queue(maxsize=maxsize or 0)
        self._time_samples = deque(maxlen=10)
        self._tasks = []

    def enable(self, recursive=True):
        if not self._tasks:
            for i in range(self.workers):
                self._tasks.append(asyncio.Task(self._run_worker()))
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
            func = self.func or self.run
            start = time.monotonic_ns()
            await func(item, outq)
            self._time_samples.append(time.monotonic_ns()-start)
            self.queue.task_done()

    @abstractmethod
    async def run(self, item, outq):
        pass

    async def __call__(self, item):
        await self.queue.put(item)

    async def join(self):
        await self.queue.join()
        if self.next:
            await self.next.queue.join()

    def execution_time(self):
        ''' Average execution time in seconds. '''
        average = sum(self._time_samples)/len(self._time_samples)
        average = average / 1000000000
        return average

    def count(self):
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