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

    def queue(self, name=None):
        """
        Get a queue object
        """
        return Queue(self)


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
