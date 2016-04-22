# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from redis.client import StrictRedis, StrictPipeline, Pipeline
from contextlib import contextmanager
import time
import math

from .compat import string_types, as_text
from .utils import import_attribute, compact, transaction
from .keys import (QUEUES_KEY, queue_name_from_key, worker_key_from_name,
                   WORKERS_KEY, queue_key_from_name)
from .exceptions import NoSuchJobError

class NoRedisConnectionException(Exception):
    pass


class RQConnection(object):
    from .queue import Queue
    from .job import Job

    queue_class = Queue
    job_class = Job

    def __init__(self, redis_conn=None, redis_kwargs={}, job_class=None,
                 queue_class=None):

        if job_class is not None:
            if isinstance(job_class, string_types):
                job_class = import_attribute(job_class)
            self.job_class = job_class

        if queue_class is not None:
            if isinstance(queue_class, string_types):
                queue_class = import_attribute(queue_class)
            self.queue_class = queue_class

        if isinstance(redis_conn, StrictRedis):
            self._redis_conn = redis_conn
        elif redis_conn is None and redis_kwargs:
            self._redis_conn = StrictRedis(**redis_kwargs)
        else:
            raise ValueError("redis argument to Connection must be of type "
                             "StrictRedis, or redis_kwargs must be provided")

        self._storage = self
        self._pipe = None

    @transaction
    def get_all_queues(self):
        """
        Returns an iterable of all Queues.
        """
        assert self._pipe is not None
        return [self.mkqueue(queue_name_from_key(rq_key)) for rq_key in
                self._smembers(QUEUES_KEY) if rq_key]

    def mkqueue(self, name='default', async=True):
        """
        Get a queue by name, note: this does not actually do a remote check. Do
        that call sync().
        """
        return self.queue_class(name, storage=self, async=async,
                                job_class=self.job_class)

    def get_queue(self, name, async=True):
        return self.mkqueue(name, async=async)

    def mkworker(self, queues, name=None, default_result_ttl=None,
                 exception_handlers=None, default_worker_ttl=None,
                 queue_class=None, job_class=None):

        from rq.worker import Worker

        if job_class is None:
            job_class = self.job_class

        if queue_class is None:
            queue_class = self.queue_class

        return Worker(queues, name=name, default_result_ttl=default_result_ttl,
                      exception_handlers=exception_handlers,
                      default_worker_ttl=default_worker_ttl,
                      queue_class=queue_class, job_class=job_class,
                      storage=self)

    def get_deferred_registry(self, name='default'):
        """

        Note: no database interaction takes place here
        """
        from .registry import DeferredJobRegistry
        return DeferredJobRegistry(name, storage=self)

    def get_started_registry(self, name='default'):
        """

        Note: no database interaction takes place here
        """
        from .registry import StartedJobRegistry
        return StartedJobRegistry(name, storage=self)

    def get_finished_registry(self, name='default'):
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

        worker_dict = self._hgetall(worker_key)
        queues = as_text(worker_dict['queues']).split(',')

        worker = Worker(queues, name=name, storage=self)
        worker._state = as_text(self._hget(worker.key, 'state') or '?')
        worker._job_id = self._hget(worker.key, 'current_job') or None

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

    def job_exists(self, job_id):
        try:
            return self.get_job(job_id)
        except NoSuchJobError:
            return None

    def clean_registries(self, queue):
        """Cleans StartedJobRegistry and FinishedJobRegistry of a queue."""
        from rq.queue import Queue
        if isinstance(queue, Queue):
            name = queue.name
        else:
            name = queue

        registry = self.get_finished_registry(name)
        registry.cleanup()
        registry = self.get_started_registry(name)
        registry.cleanup()

    def _create_job(self, *args, **kwargs):
        """
        Mostly used for testing and as a helper. Creates a job object and
        initializes it, but doesn't put it on a queue.
        """
        from .job import Job
        job = Job(storage=self)
        job._new(*args, **kwargs)
        return job

    @transaction
    def _pop_job_id_no_wait(self, queues):
        """
        Non-blocking dequeue of the next job. Job is atomically moved to running
        registry for the queue

        :param queues: list of queue names or queue objects
        :param timeout: How long to wait for a job to appear on one of the
               queues (if they are initially empty). None waits forever.
        """
        job_id = None
        ret_queue_name = None
        for ii, queue in enumerate(queues):
            if not isinstance(queue, self.queue_class):
                queue = self.get_queue(queue)

            if queue.count > 0:
                reg = self.get_started_registry(queue.name)
                job_id = as_text(self._lpop(queue.key))
                ret_queue_name = queue.name
                reg.add(job_id)
                break

        return job_id, ret_queue_name

    def _pop_job_id(self, queues, timeout=0):
        """
        Non-blocking dequeue of the next job. Job is moved to running registry
        for the queue.

        :param queues: list of queue names or queue objects
        :param timeout: How long to wait for a job to appear on one of the
               queues (if they are initially empty). None waits forever.
        :return (job_id, queue_name)
        """
        if timeout == 0:
            return self._pop_job_id_no_wait(queues)

        # Since timeout is > 0, we can use blplop. Unfortunately there is no way
        # to atomically move from a queue to a set (maybe StartedJobRegistry
        # should be a simple list and we could use BRPOPLPUSH). So there is a
        # *tiny* window of time that this job will be only stored locally in
        # memory. Generally this sort of circumstance is to be avoided since it
        # is technically possible to lose data, but in this case there doesn't
        # seem a way around it, other than busy waiting
        keys = []
        for queue in queues:
            if isinstance(queue, self.queue_class):
                keys.append(queue_key_from_name(queue.name))
            else:
                keys.append(queue_key_from_name(queue))

        # Convert None to blpop's infinite timeout (0)
        if timeout is None:
            timeout = 0
        result = self._redis_conn.blpop(keys, int(math.ceil(timeout)))

        if result:
            queue_key, job_id = result
            reg = self.get_started_registry(queue_name_from_key(queue_key))
            reg.add(job_id)
            return job_id, queue_name_from_key(queue_key)
        else:
            return None, None

    def dequeue_any(self, queues, timeout=0):
        """
        Dequeue a single job. Unlike RQ we expect all jobs to exist

        :param queues: list of queue names or queue objects
        :param timeout: How long to wait for a job to appear on one of the
               queues (if they are initially empty). None waits forever.
        """
        while True:
            start_time = time.time()
            job_id, queue_name = self._pop_job_id(queues, timeout=timeout)

            if job_id is not None:
                try:
                    return self.get_job(job_id), self.mkqueue(queue_name)
                except NoSuchJobError:
                    # If we find a job that doesn't exist, try again with timeout
                    # reduced
                    if timeout is not None:
                        timeout = max(0, timeout - (time.time() - start_time))
            else:
                return None

    def _active_transaction(self):
        return self._pipe is not None

    ### Write ###
    def _hset(self, name, key, value):
        if not self._pipe.explicit_transaction: self._pipe.multi()
        self._pipe.hset(name, key, value)

    def _setex(self, name, time, value):
        """
        Use keyword arguments so that non-strict version acts the same
        """
        if not self._pipe.explicit_transaction: self._pipe.multi()
        self._pipe.setex(name=name, time=time, value=value)

    def _lrem(self, name, count, value):
        """
        Patched count->num for non-strict version
        """
        if not self._pipe.explicit_transaction: self._pipe.multi()

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
        if not self._pipe.explicit_transaction: self._pipe.multi()
        self._pipe.zadd(name, *args, **kwargs)

    def _zrem(self, name, value):
        if not self._pipe.explicit_transaction: self._pipe.multi()
        self._pipe.zrem(name, value)

    def _lpush(self, name, *values):
        if len(values) > 0:
            if not self._pipe.explicit_transaction: self._pipe.multi()
            self._pipe.lpush(name, *values)

    def _rpush(self, name, *values):
        if not self._pipe.explicit_transaction: self._pipe.multi()
        self._pipe.rpush(name, *values)

    def _delete(self, name):
        if not self._pipe.explicit_transaction: self._pipe.multi()
        self._pipe.delete(name)

    def _srem(self, name, *values):
        if not self._pipe.explicit_transaction: self._pipe.multi()
        self._pipe.srem(name, *values)

    def _hmset(self, name, mapping):
        if not self._pipe.explicit_transaction: self._pipe.multi()
        self._pipe.hmset(name, mapping)

    def _expire(self, name, ttl):
        if not self._pipe.explicit_transaction: self._pipe.multi()
        self._pipe.expire(name, ttl)

    def _zremrangebyscore(self, *args, **kwargs):
        if not self._pipe.explicit_transaction: self._pipe.multi()
        self._pipe.zremrangebyscore(*args, **kwargs)

    def _sadd(self, name, *args, **kwargs):
        if not self._pipe.explicit_transaction: self._pipe.multi()
        self._pipe.sadd(name, *args, **kwargs)

    def _persist(self, name):
        if not self._pipe.explicit_transaction: self._pipe.multi()
        self._pipe.persist(name)

    def _hdel(self, name, key):
        if not self._pipe.explicit_transaction: self._pipe.multi()
        self._pipe.hdel(name, key)

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

    def _hget(self, name, key):
        self._pipe.watch(name)
        return self._pipe.hget(name, key)

    def _exists(self, name):
        self._pipe.watch(name)
        return self._pipe.exists(name)

    def _hexists(self, name, key):
        self._pipe.watch(name)
        return self._pipe.hexists(name, key)

    # Read / Then Write. BE WARY THIS CAN ONLY BE USED 1 TIME PER PIPE
    def _lpop(self, name):
        self._pipe.watch(name)
        out = self._pipe.lindex(name, 0)
        self._pipe.multi()
        self._pipe.lpop(name)
        return out

    @contextmanager
    def _pipeline(self):
        with self._redis_conn.pipeline() as pipe:
            yield pipe

__all__ = ['RQConnection', 'transaction']

