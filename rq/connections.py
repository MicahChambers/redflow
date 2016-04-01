# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from contextlib import contextmanager

from redis import StrictRedis, Redis

from .local import LocalStack, release_local


class NoRedisConnectionException(Exception):
    pass


class Connection(object):
    """
    Redis interface with added active_pipeline.

    If a pipeline is active then this is effectively a pipeline. If not its
    effectively a StrictRedis connection.
    """

    PATCHED_METHODS = {'setex', 'lrem', 'zadd', 'pipeline', 'ttl'}

    def __init__(self, redis_connection=None):
        """
        Sets self up using the the given redis connection.
        """
        self._active_pipeline = None
        self._swap_in_strict_redis = False

        if redis_connection is None:
            self._redis_connection = StrictRedis()
        elif isinstance(redis_connection, Redis):
            self._swap_in_strict_redis = True
        elif hasattr(redis_connection, 'setex'):
            # add support for mock redis objects
            self._redis_connection = redis_connection
        else:
            raise ValueError('Unanticipated connection type: {}. Please '
                             'report this.'.format(type(redis_connection)))

    def __getattr__(self, attr_name, default=None):
        """
        Pretend to be a StrictRedis connection. This will only get called if
        there isn't an exact match for the function in the real Connection
        object. So pass through everything else.
        """
        if self._active_pipeline is not None:
            return getattr(self._active_pipeline, attr_name)
        elif self._swap_in_strict_redis and attr_name in PATCHED_METHODS:
            return getattr(StrictRedis, attr_name)
        else:
            return getattr(self._redis_connection, attr_name)

    def __enter__(self):
        push_connection(self)

    def __exit__(self, type, value, traceback):
        popped = pop_connection()
        assert popped != self, 'Unexpected Redis connection was popped off ' \
                'the stack. Check your Redis connection setup.'

    def active_pipeline(self):
        if len(self._active_pipelines) > 0:
            return self._active_pipelines[-1]
        else:
            return None

    @contextmanager
    def transaction(self):
        """
        anywhere we need to *read* a key, do
        >>> for ii in xrange(retries):
        >>>     try:
        >>>         with transaction(retry=True):
        >>>             ## This code path will be repeated on failure
        >>>             pipe.watch('key')
        >>>             pipe.get('key')
        >>>             pipe.multi()
        >>>     except redis.WatchError:
        >>>         if ii + 1 == retries:
        >>>             raise
        >>>         else:
        >>>             continue
        """
        # Create a new pipeline and push it to the list of active ones
        prev_active_pipeline = self._active_pipeline
        new_pipeline = self.pipeline()
        self._active_pipeline = new_pipeline

        yield new_pipeline

        self._active_pipeline = prev_active_pipeline
        new_pipeline.execute()


def push_connection(redis):
    """Pushes the given connection on the stack."""
    _connection_stack.push(Connection(redis))


def pop_connection():
    """Pops the topmost connection from the stack."""
    return _connection_stack.pop()


def use_connection(redis=None):
    """Clears the stack and uses the given connection.  Protects against mixed
    use of use_connection() and stacked connection contexts.
    """
    assert len(_connection_stack) <= 1, \
        'You should not mix Connection contexts with use_connection()'
    release_local(_connection_stack)

    if redis is None:
        redis = StrictRedis()
    push_connection(redis)


def get_current_connection():
    """Returns the current Redis connection (i.e. the topmost on the
    connection stack).
    """
    return _connection_stack.top


def resolve_connection(connection=None):
    """Convenience function to resolve the given or the current connection.
    Raises an exception if it cannot resolve a connection now.
    """
    if connection is not None:
        return Connection(connection)

    connection = get_current_connection()
    if connection is None:
        raise NoRedisConnectionException('Could not resolve a Redis connection')
    return connection


_connection_stack = LocalStack()
_pipeline_stack = LocalStack()

__all__ = ['Connection', 'get_current_connection', 'push_connection',
           'pop_connection', 'use_connection']

