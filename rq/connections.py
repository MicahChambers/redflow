# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from contextlib import contextmanager

from redis import StrictRedis
from redis import Pipeline

from .compat.connections import patch_connection
from .local import LocalStack, release_local


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

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        # We shouldn't be leaving any pipes open
        assert self._active_pipe is None

    def __init__(self, conn):
        self._active_pipe = None

        if conn is None:
            self._redis_conn = StrictRedis()
        elif isinstance(conn, Redis) or isinstance(conn, Pipeline):
            raise Exception("Redis and Pipeline are not supported, use "
                            "StrictRedis and StrictPipeline")
        elif isinstance(conn, StrictRedis) or isinstance(conn, StrictPipeline):
            self._redis_conn = conn
        else:
            raise Exception("Unknown connection type provided")

    @property
    def redis_connection(self):
        if self._active_pipe:
            return self._active_pipe
        else:
            return self._redis_conn

    def get_queue(self, name=None):
        """
        Get a queue object
        """
        return Queue(self)

    def get_queues(self):
        """
        Returns an iterable of all Queues.
        """
        def to_queue(queue_key):
            return cls.from_queue_key(as_text(queue_key),
                                      connection=connection)
        return [to_queue(rq_key) for rq_key in connection.smembers(cls.redis_queues_keys) if rq_key]

    def dequeue_any(self, queues, timeout):
        """
        Class method returning the job_class instance at the front of the given
        set of Queues, where the order of the queues is important.

        When all of the Queues are empty, depending on the `timeout` argument,
        either blocks execution of this function for the duration of the
        timeout or until new messages arrive on any of the queues, or returns
        None.

        See the documentation of cls.lpop for the interpretation of timeout.
        """
        while True:
            queue_keys = [q.key for q in queues]
            result = cls.lpop(queue_keys, timeout, connection=connection)
            if result is None:
                return None
            queue_key, job_id = map(as_text, result)
            queue = cls.from_queue_key(queue_key, connection=connection)
            try:
                job = cls.job_class.fetch(job_id, connection=connection)
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
            return job, queue
        return None, None


    def get_job(self, job_id, connection=None):
        """Fetches a persisted job from its corresponding Redis key and
        instantiates it.
        """
        job = Job(job_id, connection=connection)
        try:
            job.refresh()
        except NoSuchJobError:
            return None
        return job

    def cancel_job(self, job_id):
        """Cancels the job with the given job ID, preventing execution.  Discards
        any job info (i.e. it can't be requeued later).
        """
        Job(job_id, connection=connection).cancel()

    def requeue_job(self, job_id):
        """Requeues the job with the given job ID.  If no such job exists, just
        remove the job ID from the failed queue, otherwise the job ID should refer
        to a failed job (i.e. it should be on the failed queue).
        """
        from .queue import get_failed_queue
        fq = get_failed_queue(connection=connection)
        fq.requeue(job_id)

    # Job construction
    def _create_job(self, func, args=None, kwargs=None, result_ttl=None,
                    ttl=None, status=None, description=None, depends_on=None,
                    timeout=None, id=None, origin=None, meta=None):
        """Creates a new Job instance for the given function, arguments, and
        keyword arguments.
        """
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}

        if not isinstance(args, (tuple, list)):
            raise TypeError('{0!r} is not a valid args list'.format(args))
        if not isinstance(kwargs, dict):
            raise TypeError('{0!r} is not a valid kwargs dict'.format(kwargs))

        job = Job(connection=connection)
        if id is not None:
            job.set_id(id)

        if origin is not None:
            job.origin = origin

        # Set the core job tuple properties
        job._instance = None
        if inspect.ismethod(func):
            job._instance = func.__self__
            job._func_name = func.__name__
        elif inspect.isfunction(func) or inspect.isbuiltin(func):
            job._func_name = '{0}.{1}'.format(func.__module__, func.__name__)
        elif isinstance(func, string_types):
            job._func_name = as_text(func)
        elif not inspect.isclass(func) and hasattr(func, '__call__'):  # a callable class instance
            job._instance = func
            job._func_name = '__call__'
        else:
            raise TypeError('Expected a callable or a string, but got: {}'.format(func))
        job._args = args
        job._kwargs = kwargs

        # Extra meta data
        job.description = description or job.get_call_string()
        job.result_ttl = result_ttl
        job.ttl = ttl
        job.timeout = timeout
        job._status = status
        job.meta = meta or {}

        # dependencies could be a single job or a list of jobs
        if depends_on:
            if isinstance(depends_on, list):
                job._dependency_ids = [tmp.id for tmp in depends_on]
            else:
                job._dependency_ids = ([depends_on.id]
                    if isinstance(depends_on, Job) else [depends_on])

        return job

    def get_failed_queue(self):
        """Returns a handle to the special failed queue."""
        return FailedQueue(connection=self.connection)

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
