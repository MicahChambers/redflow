# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from contextlib import contextmanager

from redis import StrictRedis

from .compat.connections import patch_connection
from .local import LocalStack, release_local


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

                    except WatchError as exc:
                        pass

                # transaction successful!
                self._storage._pipe = None

        return out


class RQConnection(object):

    def __init__(self, *args, **kwargs):
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
        self._pipe.watch(queues_key())
        return [queue_name(rq_key) for rq_key in
                self._smembers(queues_key()) if rq_key]

    def mkqueue(self, name):
        """
        Get a queue by name, note: this does not actually do a remote check. Do
        that call sync().
        """
        name = queue_key[len(prefix):]
        from rq.queue import Queue
        return Queue(name, storage=self)

    def get_queue(self, name):
        return self.mkqueue(name)

    def get_deferred_registery(self, name):
        return DeferredJobRegistry(self.origin, storage=self)

    @transaction
    def get_job(self, job_id):
        job = Job(job_id, storage=self)
        job.refresh()
        return job

    def _active_transaction():
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

__all__ = ['RQConnection', 'transaction']

