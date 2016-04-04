# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from contextlib import contextmanager

from redis import StrictRedis
from redis import Pipeline

from rq.compat import as_text
from rq.exceptions import DequeueTimeout
from rq.compat.connections import patch_connection
from rq.local import LocalStack, release_local


class NoRedisConnectionException(Exception):
    pass


def RQConnection(object):
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
        elif isinstance(conn, Redis) or isinstance(conn, Pipeline):
            raise Exception("Redis and Pipeline are not supported, use "
                            "StrictRedis and StrictPipeline")
        elif isinstance(conn, StrictRedis):
            self._using_strict_redis = True
            self._redis_conn = conn
        else:
            raise Exception("Unknown connection type provided")

    def get_queue(self, name=None, default_timeout=None, async=True,
                  job_class=None):
        """
        Get a queue object. Note that this doesn't actually hit the redis
        backend and using the queue will implicity create it.
        """
        # TODO maybe sync configs from keys? Configs might not be stored in
        # redis though
        if name is None:
            return Queue(default_timeout=default_timeout, async=async,
                         job_class=job_class, connection=self)
        else:
            return Queue(name=name, default_timeout=default_timeout,
                         async=async, job_class=job_class, connection=self)

    def get_queues(self):
        """
        Returns an iterable of all Queues.
        """
        from rq.queue import queue_key_to_name
        from rq.queue import REDIS_QUEUES_KEYS
        lst = []
        for rq_key in _redis_conn.smembers(REDIS_QUEUES_KEYS):
            if rq_key:  # TODO does this ever evaluate to False?
                q = get_queue(queue_key_to_name(rq_key))
                lst.append[q]
        return lst

    def get_job(self, job_id):
        """
        Fetches a persisted job from its corresponding Redis key and
        instantiates it.
        """
        job = Job(job_id, connection=self)
        try:
            job.refresh()
        except NoSuchJobError:
            return None
        return job

    def cancel_job(self, job_id):
        """Cancels the job with the given job ID, preventing execution.  Discards
        any job info (i.e. it can't be requeued later).
        """
        Job(job_id, connection=self).cancel()

    def requeue_job(self, job_id):
        """Requeues the job with the given job ID.  If no such job exists, just
        remove the job ID from the failed queue, otherwise the job ID should refer
        to a failed job (i.e. it should be on the failed queue).
        """
        fq = self.get_failed_queue()
        fq.requeue(job_id)

    def get_failed_queue(self):
        """Returns a handle to the special failed queue."""
        return FailedQueue(connection=self)

    def dequeue_any(self, queues, timeout):
        """
        Returns job_class instance at the front of the given set of Queues,
        where the order of the queues is important.

        When all of the Queues are empty, depending on the `timeout` argument,
        either blocks execution of this function for the duration of the
        timeout or until new messages arrive on any of the queues, or returns
        None.

        See the documentation of cls.lpop for the interpretation of timeout.
        """
        from rq.queue import Queue
        from rq.queue import queue_name_to_key
        from rq.queue import queue_key_to_name

        # Resolve queue keys
        queue_keys = []
        for q in queues:
            if isinstance(q, basestring):
                queue_keys.append(queue_name_to_key(q))
            elif isinstance(q, Queue):
                queue_keys.append(q.key)

        while True:
            result = self._lpop(queue_keys, timeout, connection=connection)
            if result is None:
                return None

            # Convert queue key and job id back into Queue and Job objects
            try:
                job = Job(job_id, connection=self)
                job.refresh()
            except NoSuchJobError:
                # Silently pass on jobs that don't exist (anymore),
                # and continue in the look
                continue
            except UnpickleError as e:
                # Attach queue information on the exception for improved error
                # reporting
                e.job_id = job_id
                e.queue = queue
                raise e

            queue_key, job_id = map(as_text, result)
            queue = Queue(queue_key_to_name(queue_key), connection=self)

            return job, queue
        return None, None

    ##
    # Wrappers around needed redis functions to ensure consistent interface,
    # and so pipelines can be used transparently
    ##
    def _lpop(self, queue_keys, timeout):
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
        if timeout is not None:  # blocking variant
            if timeout == 0:
                raise ValueError('RQ does not support indefinite timeouts. '
                                 'Please pick a timeout value > 0')
            result = _redis_conn.blpop(queue_keys, timeout)
            if result is None:
                raise DequeueTimeout(timeout, queue_keys)
            queue_key, job_id = result
            return queue_key, job_id
        else:  # non-blocking variant
            for queue_key in queue_keys:
                blob = _redis_conn.lpop(queue_key)
                if blob is not None:
                    return queue_key, blob
            return None

    def _setex(self, name, value, time):
        return self._redis_conn(name=name, value=value, time=time)

    def _lrem(self, name, count, value):
        if self._using_strict_redis:
            return self._redis_conn(name=name, count=count, value=value)
        else:
            return self._redis_conn(name=name, num=count, value=value)

    def _zadd(self, name, *args, **kwargs):
        """
        Intermediate for
        """
        if self._using_strict_redis:
            # The redis connection is struct
            return self._redis_conn(name, *args, **kwargs)
        else:
            # swap order so that order is correct for non-strict redis conn
            swapped_args = [name, score for score, name in args]
            return self._redis_conn(name, *swapped_args, **kwargs)

    def __getattr__(self, attr_name, defvalue):
        """
        Passes through unmodified redis functions if possible
        """
        if attr_name.startswith('_') and hasattr(self._redis_conn, attr_name[1:]):
            return getattr(self._redis_conn, attr_name[1:])

__all__ = ['Connection']


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
