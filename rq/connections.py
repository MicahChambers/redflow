# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from redis import StrictRedis
from redis import Redis

from .utils import compact
from .keys import (queue_name_to_key, worker_name_to_key, worker_key_to_name,
                   queue_key_to_name, REDIS_WORKERS_KEY, REDIS_QUEUES_KEY)
from .queue import Queue
from .job import Job
from .worker import Worker
from .exceptions import NoSuchJobError, UnpickleError
from .compat import as_text
from .exceptions import DequeueTimeout
from .queue import FailedQueue


class NoRedisConnectionException(Exception):
    pass


class RQConnection(object):
    """
    RQ Connection Class. Main usage:

        >>> with Connection(StrictRedis(...)) as conn:
        >>>     q1 = conn.queue("my-queue-1")
        >>>     job1 = q1.enqueue_job(foo, bar=10)
        >>>     job2 = q1.enqueue_job(bar, gam=23)
        >>>     q2 = conn.queue("my-queue2")
        >>>     job3 = q2.enqueue_job(gar, hello='world', depeneds_on=[job1, job2])
    """
    def __init__(self, conn):
        if conn is None:
            self._redis_conn = StrictRedis()
            self._using_strict_redis = True
        elif isinstance(conn, Redis):
            raise Exception("Connection Redis is not supported, use StrictRedis ")
        elif isinstance(conn, StrictRedis):
            self._using_strict_redis = True
            self._redis_conn = conn
        else:
            raise Exception("Unknown connection type provided")

    def ping(self):
        """
        Check that the connection is active
        """
        return self._redis_conn.ping()

    def get_queue(self, name=None, default_timeout=None, async=True):
        """
        Get a queue object. Note that this doesn't actually hit the redis
        backend and using the queue will implicity create it.
        """
        # TODO maybe sync configs from keys? Configs might not be stored in
        # redis though
        from rq.queue import Queue
        if name is None:
            return Queue(default_timeout=default_timeout, async=async,
                         connection=self)
        else:
            return Queue(name=name, default_timeout=default_timeout,
                         async=async, connection=self)

    def get_workers(self):
        """
        Returns an iterable of all Workers.
        """
        reported_working = self._smembers(REDIS_WORKERS_KEY)
        workers = [self.get_worker(worker_key_to_name(as_text(key)))
                   for key in reported_working]
        return compact(workers)

    def get_worker(self, worker_name):
        """
        Returns a Worker instance
        """
        worker_key = worker_name_to_key(worker_name)
        if not self._exists(worker_key):
            self._srem(REDIS_WORKERS_KEY, worker_key)
            return None

        worker = Worker(worker_name, connection=self)
        queues = as_text(self._hget(worker.key, 'queues'))
        worker._state = as_text(self._hget(worker.key, 'state') or '?')
        worker._job_id = self._hget(worker.key, 'current_job') or None
        if queues:
            worker.queues = [Queue(queue, connection=self)
                             for queue in queues.split(',')]
        return worker

    def get_queues(self):
        """
        Returns an iterable of all Queues.
        """
        lst = []
        for rq_key in self._smembers(REDIS_QUEUES_KEY):
            if rq_key:  # TODO does this ever evaluate to False?
                q = self.get_queue(queue_key_to_name(rq_key))
                lst.append(q)
        return lst

    def get_job(self, job_id):
        """
        Fetches a persisted job from its corresponding Redis key and
        instantiates it.
        """
        job = Job(connection=self, id=job_id)
        try:
            job.refresh()
        except NoSuchJobError:
            return None
        return job

    def get_failed_queue(self):
        """Returns a handle to the special failed queue."""
        return FailedQueue(connection=self)

    def dequeue_any(self, queues, timeout):
        """
        Returns Job instance at the front of the given set of Queues,
        where the order of the queues is important.

        When all of the Queues are empty, depending on the `timeout` argument,
        either blocks execution of this function for the duration of the
        timeout or until new messages arrive on any of the queues, or returns
        None.

        See the documentation of cls.lpop for the interpretation of timeout.
        """

        # Resolve queue keys
        queue_keys = []
        for q in queues:
            if isinstance(q, basestring):
                queue_keys.append(queue_name_to_key(q))
            elif isinstance(q, Queue):
                queue_keys.append(q.key)

        while True:
            result = self._pop_from_one(queue_keys, timeout)
            if result is None:
                return None

            # Convert queue key and job id back into Queue and Job objects
            try:
                queue_key, job_id = result
                job = Job(job_id, connection=self)
                job.refresh()
            except NoSuchJobError:
                # Silently pass on jobs that don't exist (anymore),
                continue
            except UnpickleError as e:
                # Attach queue information on the exception for improved error
                # reporting
                e.job_id = job_id
                e.queue = queue_key_to_name(queue_key)
                raise e

            queue_key, job_id = map(as_text, result)
            queue = Queue(queue_key_to_name(queue_key), connection=self)

            return job, queue
        return None, None

    ##
    # Wrappers around needed redis functions to ensure consistent interface,
    # and so pipelines can be used transparently
    ##
    def _pop_from_one(self, queue_keys, timeout=None):
        """
        Helper method. Intermediate method to abstract away from some
        Redis API details.

        If timeout is not None then the blocking version of lpop (blpop) is
        used, otherwise this will return None if all the queues are empty.

        The issue:
            LPOP (the non-blocking variant) accepts only a single key
            BLPOP accepts multiple but has to have a timeout >= 1 (0 blocks
                   forever)

        To get non-blocking LPOP, we need to iterate over all queues, do
        individual LPOPs, and return the result. Until Redis receives a specific
        method for this, we'll have to wrap it this way.

        :param list queue_keys: List of keys for redis lists to pope out of.
        :param int timeout:
            None - non-blocking (return immediately)
             > 0 - maximum number of seconds to block
        """
        if isinstance(queue_keys, basestring):
            queue_keys = [queue_keys]

        if timeout is not None:  # blocking variant
            if timeout == 0:
                raise ValueError('RQ does not support indefinite timeouts. '
                                 'Please pick a timeout value > 0')
            result = self._redis_conn.blpop(queue_keys, timeout)
            if result is None:
                raise DequeueTimeout(timeout, queue_keys)
            queue_key, job_id = result
            return queue_key, job_id
        else:  # non-blocking variant
            for queue_key in queue_keys:
                blob = self._redis_conn.lpop(queue_key)
                if blob is not None:
                    return queue_key, blob
            return None

    def _setex(self, name, value, time):
        return self._redis_conn(name=name, value=value, time=time)

    def _lrem(self, name, count, value):
        if self._using_strict_redis:
            return self._redis_conn.lrem(name=name, count=count, value=value)
        else:
            return self._redis_conn.lrem(name=name, num=count, value=value)

    def _zadd(self, name, *args, **kwargs):
        """
        Intermediate for
        """
        if self._using_strict_redis:
            # The redis connection is struct
            return self._redis_conn.zadd(name, *args, **kwargs)
        else:
            # swap order so that order is correct for non-strict redis conn
            swapped_args = [(key, score) for score, key in args]
            return self._redis_conn.zadd(name, *swapped_args, **kwargs)

    def __getattr__(self, attr_name):
        """
        Passes through unmodified redis functions if possible
        """
        if attr_name.startswith('_') and hasattr(self._redis_conn, attr_name[1:]):
            return getattr(self._redis_conn, attr_name[1:])
        else:
            raise AttributeError('{} has no attribute "{}"'.format(type(self),
                                                                   attr_name))

__all__ = ['RQConnection']

#def push_connection(redis):
#    """Pushes the given connection on the stack."""
#    _connection_stack.push(patch_connection(redis))
#
#
#def pop_connection():
#    """Pops the topmost connection from the stack."""
#    return _connection_stack.pop()
#
#
#def use_connection(redis=None):
#    """Clears the stack and uses the given connection.  Protects against mixed
#    use of use_connection() and stacked connection contexts.
#    """
#    assert len(_connection_stack) <= 1, \
#        'You should not mix Connection contexts with use_connection()'
#    release_local(_connection_stack)
#
#    if redis is None:
#        redis = StrictRedis()
#    push_connection(redis)
#
#
#def get_current_connection():
#    """Returns the current Redis connection (i.e. the topmost on the
#    connection stack).
#    """
#    return _connection_stack.top
#
#
#def resolve_connection(connection=None):
#    """Convenience function to resolve the given or the current connection.
#    Raises an exception if it cannot resolve a connection now.
#    """
#    if connection is not None:
#        return patch_connection(connection)
#
#    connection = get_current_connection()
#    if connection is None:
#        raise NoRedisConnectionException('Could not resolve a Redis connection')
#    return connection
#
#
#_connection_stack = LocalStack()
#
#__all__ = ['Connection', 'get_current_connection', 'push_connection',
#           'pop_connection', 'use_connection']
