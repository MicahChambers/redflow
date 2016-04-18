# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from functools import wraps
from redis import StrictRedis, WatchError, StrictPipeline, Pipeline

from .compat import string_types, as_text
from .utils import import_attribute, compact
from .keys import (QUEUES_KEY, queue_name_from_key, worker_key_from_name,
                   WORKERS_KEY, queue_key_from_name)


class NoRedisConnectionException(Exception):
    pass


def transaction(wrapped):
    """
    If the specified RQConnection already contains a transaction use it,
    otherwise, create a new transaction, which will end at the end of the
    wrapped function.  The entire function will be retried in case of WatchError
    error.

    Usage:

    >>> @transaction
    >>> def foo(self, arg):
    >>>     blah
    """

    @wraps(wrapped)
    def func(self, *args, **kwargs):
        assert hasattr(self, '_storage')
        assert isinstance(self._storage, RQConnection)

        if self._storage._active_transaction():
            return wrapped(self, *args, **kwargs)
        else:
            with self._storage.pipline() as pipe:
                self._storage._pipe = pipe
                while True:
                    try:
                        out = self.wrapped(*args, **kwargs)
                        pipe.execute()

                    except WatchError:
                        pass

                # transaction successful!
                self._storage._pipe = None

        return out


class RQConnection(object):
    from .queue import Queue
    from .job import Job

    queue_class = Queue
    job_class = Job

    def __init__(self, job_class=None, queue_class=None, *args, **kwargs):

        if job_class is not None:
            if isinstance(job_class, string_types):
                job_class = import_attribute(job_class)
            self.job_class = job_class

        if queue_class is not None:
            if isinstance(queue_class, string_types):
                queue_class = import_attribute(queue_class)
            self.queue_class = queue_class

        if 'redis' in kwargs:
            self._redis_conn = kwargs['redis']
        else:
            self._redis_conn = StrictRedis(*args, **kwargs)

        if not isinstance(self._redis_conn, StrictRedis):
            raise ValueError("redis argument to Connection must be of type "
                             "StrictRedis")

        self._storage = self
        self._pipe = None

    def __enter__(self):
        assert self._pipe is None
        self._pipe = self._redis_conn.pipeline()

    def __exit(self, type, value, traceback):
        pipe = self._pipe
        self._pipe = None
        pipe.execute()

    def all_queues(self):
        """
        Returns an iterable of all Queues.
        """
        assert self._pipe is not None
        self._pipe.watch(QUEUES_KEY)
        return [queue_name_from_key(rq_key) for rq_key in
                self._smembers(QUEUES_KEY) if rq_key]

    def mkqueue(self, name):
        """
        Get a queue by name, note: this does not actually do a remote check. Do
        that call sync().
        """
        return self.queue_class(name, storage=self)

    def get_queue(self, name):
        return self.mkqueue(name)

    def get_deferred_registery(self, name):
        """

        Note: no database interaction takes place here
        """
        from .registry import DeferredJobRegistry
        return DeferredJobRegistry(name, storage=self)

    def get_started_registry(self, name):
        """

        Note: no database interaction takes place here
        """
        from .registry import StartedJobRegistry
        return StartedJobRegistry(name, storage=self)

    def get_finished_registry(self, name):
        """

        Note: no database interaction takes place here
        """
        from .registry import FinishedJobRegistry
        return FinishedJobRegistry(name, storage=self)

    def get_failed_queue(self):
        """Returns a handle to the special failed queue."""
        from .queue import FailedQueue
        return FailedQueue(storage=self)

    @transaction
    def get_worker(self, name):
        """
        Returns a Worker instance
        """
        from .worker import Worker

        worker_key = worker_key_from_name(name)
        if not self._exists(worker_key):
            self._srem(WORKERS_KEY, worker_key)
            return None

        worker = Worker(name, storage=self)
        queues = as_text(self._hget(worker.key, 'queues'))
        worker._state = as_text(self._hget(worker.key, 'state') or '?')
        worker._job_id = self._hget(worker.key, 'current_job') or None
        if queues:
            worker.queues = [self.mkqueue(queue, storage=self)
                             for queue in queues.split(',')]
        return worker

    @transaction
    def get_all_workers(self):
        """
        Returns an iterable of all Workers.
        """
        reported_working = self._storage._smembers(WORKERS_KEY)
        workers = [self.get_worker(name) for name in reported_working]
        return compact(workers)

    @transaction
    def get_job(self, job_id):
        job = self.job_class(job_id, storage=self)
        job.refresh()
        return job

    @transaction
    def get_next_job_id(self, queues):
        """
        Non-blocking dequeue of the next job. Job is atomically moved to running
        registry for the queue

        :param queues: list of queue names or queue objects
        :param timeout: How long to wait for a job to appear on one of the
               queues (if they are initially empty). None waits forever.
        """
        job_id = None
        for ii, queue in enumerate(queues):
            if not isinstance(queue, self.queue_class):
                queue = self.get_queue(queue)

            if queue.count() > 0:
                reg = self.get_started_registry(queue.name)
                job_id = as_text(self._lpop(queue.key))
                reg.add(job_id)
                break

        return job_id

    def wait_for_next_job_id(self, queues, timeout=None):
        """
        Non-blocking dequeue of the next job. Job is moved to running registry
        for the queue.

        :param queues: list of queue names or queue objects
        :param timeout: How long to wait for a job to appear on one of the
               queues (if they are initially empty). None waits forever.
        """
        if timeout == 0:
            return self.get_next_job(queues)

        # Since timeout is > 0, we can use blplop. Unfortunately there is no way
        # to atomically move from a queue to a set (maybe StartedJobRegistry
        # should be a simple list). So there is a *tiny* window of time that
        # this job will be only stored locally in memory. Generally this sort of
        # circumstance is to be avoided but in this case there doesn't seem a
        # way around it, other than busy waiting
        keys = []
        for queue in queues:
            if isinstance(queue, self.queue_class):
                keys.append(queue_key_from_name(queue.name))
            else:
                keys.append(queue_key_from_name(queue))

        result = self._redis_conn.blpop(keys)

        if result:
            queue_key, job_id = result
            reg = self.get_started_registry(queue_name_from_key(queue_key))
            reg.add(job_id)
            return job_id
        else:
            return None

    def dequeue_any(self, queues, timeout):
        """
        Dequeue a single job. Unlike RQ we expect all jobs to exist

        :param queues: list of queue names or queue objects
        :param timeout: How long to wait for a job to appear on one of the
               queues (if they are initially empty). None waits forever.
        """
        job_id = self.wait_for_next_job_id(queues, timeout)
        return self.get_job(job_id)

    def _active_transaction(self):
        return self._pipe is not None

    ### Write ###
    def _hset(self, name, key, value):
        self._pipe.multi()
        self._pipe.hset(name, key, value)

    def _setex(self, name, time, value):
        """
        Use keyword arguments so that non-strict version acts the same
        """
        self._pipe.multi()
        self._pipe.setex(name=name, time=time, value=value)

    def _lrem(self, name, count, value):
        """
        Patched count->num for non-strict version
        """
        self._pipe.multi()

        if isinstance(self._pipe, StrictPipeline):
            # name, count, value
            self._pipe.lrem(name, count, value)

        elif isinstance(self._pipe, Pipeline):
            # name, value, num
            self._pipe.lrem(name, num=count, value=value)

    def _zadd(self, name, *args, **kwargs):
        """
        Patched to handle [score0, name0, score1, name1, ... ] in args for
        non-strict version
        """
        self._pipe.multi()
        StrictPipeline.zadd(self._pipe, name, *args, **kwargs)

    def _zrem(self, name, value):
        self._pipe.multi()
        self._pipe.zrem(name, value)

    def _lpush(self, name, *values):
        self._pipe.multi()
        self._pipe.lpush(*values)

    def _rpush(self, name, *values):
        self._pipe.multi()
        self._pipe.rpush(*values)

    def _delete(self, name):
        self._pipe.multi()
        self._pipe.delete(name)

    def _srem(self, name, *values):
        self._pipe.multi()
        self._pipe.srem(name, *values)

    def _hmset(self, name, mapping):
        self._pipe.multi()
        self._pipe.hmset(name, mapping)

    def _expire(self, name, ttl):
        self._pipe.multi()
        self._pipe.expire(name, ttl)

    def _zremrangebyscore(self, *args, **kwargs):
        self._pipe.multi()
        self._pipe.zremrangebyscore(*args, **kwargs)

    def _sadd(self, name, *args, **kwargs):
        self._pipe.multi()
        self._pipe.sadd(*args, **kwargs)

    ### Read ###
    def _ttl(self, name):
        """
        Return the strict -1 if no ttl exists for the key
        """
        self._pipe.watch(name)
        out = self._pipe.ttl(name)
        if out is None:
            return -1
        return out

    def _pttl(self, name):
        """
        Return the strict -1 if no ttl exists for the key
        """
        self._pipe.watch(name)
        out = self._pipe.pttl(name)
        if out is None:
            return -1
        return out

    def _smembers(self, name):
        self._pipe.watch(name)
        return self._pipe.smembers(name)

    def _llen(self, name):
        self._pipe.watch(name)
        return self._pipe.llen(name)

    def _lrange(self, name, start, end):
        self._pipe.watch(name)
        return self._pipe.lrange(name, start, end)

    def _zcard(self, name):
        self._pipe.watch(name)
        return self._pipe.zcard(name)

    def _zrangebyscore(self, name, *args, **kwargs):
        self._pipe.watch(name)
        return self._pipe.zrangebyscore(name, *args, **kwargs)

    def _zrange(self, name, *args, **kwargs):
        self._pipe.watch(name)
        return self._pipe.zrange(name, *args, **kwargs)

    def _scard(self, name):
        self._pipe.watch(name)
        return self._pipe.scard(name)

    def _hgetall(self, name):
        self._pipe.watch(name)
        return self._pipe.hgetall(name)

    # Read / Then Write. BE WARY THIS CAN ONLY BE USED 1 TIME PER PIPE
    def _lpop(self, name):
        self._pipe.watch(name)
        out = self._pipe.lindex(name, 0)
        self._pipe.multi()
        return out

__all__ = ['RQConnection', 'transaction']

