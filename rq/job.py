# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import inspect
import warnings
from functools import partial
from uuid import uuid4
from redis import WatchError

from .compat import as_text, decode_redis_hash, string_types, text_type
from .exceptions import NoSuchJobError, UnpickleError
from .utils import enum, import_attribute, utcformat, utcnow, utcparse
from .keys import (key_for_job, dependents_key_for_job, key_for_dependents,
                   queue_name_to_key)

try:
    import cPickle as pickle
except ImportError:  # noqa
    import pickle

# Serialize pickle dumps using the highest pickle protocol (binary, default
# uses ascii)
dumps = partial(pickle.dumps, protocol=pickle.HIGHEST_PROTOCOL)
loads = pickle.loads


JobStatus = enum(
    'JobStatus',
    QUEUED='queued',
    FINISHED='finished',
    FAILED='failed',
    STARTED='started',
    DEFERRED='deferred'
)

# Sentinel value to mark that some of our lazily evaluated properties have not
# yet been evaluated.
UNEVALUATED = object()


def unpickle(pickled_string):
    """Unpickles a string, but raises a unified UnpickleError in case anything
    fails.

    This is a helper method to not have to deal with the fact that `loads()`
    potentially raises many types of exceptions (e.g. AttributeError,
    IndexError, TypeError, KeyError, etc.)
    """
    try:
        obj = loads(pickled_string)
    except Exception as e:
        raise UnpickleError('Could not unpickle', pickled_string, e)
    return obj


class Job(object):
    """
    A Job is just a convenient datastructure to pass around job (meta) data.
    """

    def __init__(self, id=None, connection=None):
        from rq.connections import RQConnection
        if isinstance(connection, RQConnection):
            self._connection = connection
        else:
            self._connection = RQConnection(connection)

        if id is None:
            self._id = text_type(uuid4())
        else:
            self._id = id

        self.created_at = utcnow()
        self._data = UNEVALUATED
        self._func_name = UNEVALUATED
        self._instance = UNEVALUATED
        self._args = UNEVALUATED
        self._kwargs = UNEVALUATED
        self.description = None
        self.origin = None
        self.enqueued_at = None
        self.started_at = None
        self.ended_at = None
        self._result = None
        self.exc_info = None
        self.timeout = None
        self.result_ttl = None
        self.ttl = None
        self._status = None
        self._dependency_ids = None
        self.meta = {}

    def create(self, func, args=None, kwargs=None, result_ttl=None,
               ttl=None, status=None, description=None, depends_on=None,
               timeout=None, origin=None, meta=None):
        """
        Actually fills in parameters. Similar to refresh, but instead of pulling
        remote properties sets new propertys. Note that the result is not saved,
        so if you want the updated job to persist you must call:

            >>> job.create(...)
            >>> job.save()
        """
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}

        if not isinstance(args, (tuple, list)):
            raise TypeError('{0!r} is not a valid args list'.format(args))
        if not isinstance(kwargs, dict):
            raise TypeError('{0!r} is not a valid kwargs dict'.format(kwargs))

        if origin is not None:
            self.origin = origin

        # Set the core job tuple properties
        self._instance = None
        if inspect.ismethod(func):
            self._instance = func.__self__
            self._func_name = func.__name__
        elif inspect.isfunction(func) or inspect.isbuiltin(func):
            self._func_name = '{0}.{1}'.format(func.__module__, func.__name__)
        elif isinstance(func, string_types):
            self._func_name = as_text(func)
        elif not inspect.isclass(func) and hasattr(func, '__call__'):  # a callable class instance
            self._instance = func
            self._func_name = '__call__'
        else:
            raise TypeError('Expected a callable or a string, but got: {}'.format(func))
        self._args = args
        self._kwargs = kwargs

        # Extra meta data
        self.description = description or self.get_call_string()
        self.result_ttl = result_ttl
        self.ttl = ttl
        self.timeout = timeout
        self._status = status
        self.meta = meta or {}

        # dependencies could be a single job or a list of jobs
        if depends_on:
            if isinstance(depends_on, list):
                self._dependency_ids = [tmp.id for tmp in depends_on]
            elif isinstance(depends_on, Job):
                self._dependency_ids = [depends_on.id]
            else:
                self._dependency_ids = [depends_on]
        else:
            self._dependency_ids = []

        return self

    @property
    def redis(self):
        return self._connection._redis_conn

    @property
    def id(self):
        return self._id

    def get_status(self):
        self._status = as_text(self.redis.hget(self.key, 'status'))
        return self._status

    def _get_status(self):
        warnings.warn(
            "job.status is deprecated. Use job.get_status() instead",
            DeprecationWarning
        )
        return self.get_status()

    def set_status(self, status, pipeline=None):
        self._status = status
        self.redis.hset(self.key, 'status', self._status)

    def _set_status(self, status):
        warnings.warn(
            "job.status is deprecated. Use job.set_status() instead",
            DeprecationWarning
        )
        self.set_status(status)

    status = property(_get_status, _set_status)

    @property
    def is_finished(self):
        return self.get_status() == JobStatus.FINISHED

    @property
    def is_queued(self):
        return self.get_status() == JobStatus.QUEUED

    @property
    def is_failed(self):
        return self.get_status() == JobStatus.FAILED

    @property
    def is_started(self):
        return self.get_status() == JobStatus.STARTED

    @property
    def dependencies(self):
        """
        Returns a list of job's dependencies. To avoid repeated
        Redis fetches, we cache job.dependencies
        """
        if self._dependency_ids is None:
            return None
        if hasattr(self, '_dependencies'):
            return self._dependencies
        self._dependencies = [
            self._connection.get_job(dependency_id)
            for dependency_id in self._dependency_ids
        ]

        return self._dependencies

    @property
    def dependents(self):
        """
        Returns a list of jobs whose execution depends on this
        job's successful execution
        """
        dependents_ids = self.redis.smembers(self.dependents_key)
        return [self._connection.get_job(id) for id in dependents_ids]

    @property
    def func(self):
        func_name = self.func_name
        if func_name is None:
            return None

        if self.instance:
            return getattr(self.instance, func_name)

        return import_attribute(self.func_name)

    def _unpickle_data(self):
        self._func_name, self._instance, self._args, self._kwargs = unpickle(self.data)

    @property
    def data(self):
        if self._data is UNEVALUATED:
            if self._func_name is UNEVALUATED:
                raise ValueError('Cannot build the job data')

            if self._instance is UNEVALUATED:
                self._instance = None

            if self._args is UNEVALUATED:
                self._args = ()

            if self._kwargs is UNEVALUATED:
                self._kwargs = {}

            job_tuple = self._func_name, self._instance, self._args, self._kwargs
            self._data = dumps(job_tuple)
        return self._data

    @data.setter
    def data(self, value):
        self._data = value
        self._func_name = UNEVALUATED
        self._instance = UNEVALUATED
        self._args = UNEVALUATED
        self._kwargs = UNEVALUATED

    @property
    def func_name(self):
        if self._func_name is UNEVALUATED:
            self._unpickle_data()
        return self._func_name

    @func_name.setter
    def func_name(self, value):
        self._func_name = value
        self._data = UNEVALUATED

    @property
    def instance(self):
        if self._instance is UNEVALUATED:
            self._unpickle_data()
        return self._instance

    @instance.setter
    def instance(self, value):
        self._instance = value
        self._data = UNEVALUATED

    @property
    def args(self):
        if self._args is UNEVALUATED:
            self._unpickle_data()
        return self._args

    @args.setter
    def args(self, value):
        self._args = value
        self._data = UNEVALUATED

    @property
    def kwargs(self):
        if self._kwargs is UNEVALUATED:
            self._unpickle_data()
        return self._kwargs

    @kwargs.setter
    def kwargs(self, value):
        self._kwargs = value
        self._data = UNEVALUATED

    def __repr__(self):  # noqa
        return 'Job({0!r}, enqueued_at={1!r})'.format(self._id, self.enqueued_at)

    @property
    def key(self):
        """The Redis key that is used to store job hash under."""
        return key_for_job(self.id)

    @property
    def dependents_key(self):
        """The Redis key that is used to store job dependents hash under."""
        return dependents_key_for_job(self.id)

    @property
    def result(self):
        """
        Returns the return value of the job.

        Initially, right after enqueueing a job, the return value will be
        None.  But when the job has been executed, and had a return value or
        exception, this will return that value or exception.

        Note that, when the job has no return value (i.e. returns None), the
        ReadOnlyJob object is useless, as the result won't be written back to
        Redis.

        Also note that you cannot draw the conclusion that a job has _not_
        been executed when its return value is None, since return values
        written back to Redis will expire after a given amount of time (500
        seconds by default).
        """
        if self._result is None:
            rv = self.redis.hget(self.key, 'result')
            if rv is not None:
                # cache the result
                self._result = loads(rv)
        return self._result

    """Backwards-compatibility accessor property `return_value`."""
    return_value = result

    # Persistence
    def refresh(self):  # noqa
        """
        Overwrite the current instance's properties with the values in the
        corresponding Redis key.

        Will raise a NoSuchJobError if no corresponding Redis key exists.
        """
        key = self.key
        obj = decode_redis_hash(self.redis.hgetall(key))
        if len(obj) == 0:
            raise NoSuchJobError('No such job: {0}'.format(key))

        try:
            self.data = obj['data']
        except KeyError:
            raise NoSuchJobError('Unexpected job format: {0}'.format(obj))

        self.created_at = utcparse(as_text(obj.get('created_at')))
        self.origin = as_text(obj.get('origin'))
        self.description = as_text(obj.get('description'))
        self.enqueued_at = utcparse(as_text(obj.get('enqueued_at')))
        self.started_at = utcparse(as_text(obj.get('started_at')))
        self.ended_at = utcparse(as_text(obj.get('ended_at')))
        self._result = unpickle(obj.get('result')) if obj.get('result') else None  # noqa
        self.exc_info = as_text(obj.get('exc_info'))
        self.timeout = int(obj.get('timeout')) if obj.get('timeout') else None
        self.result_ttl = int(obj.get('result_ttl')) if obj.get('result_ttl') else None  # noqa
        self._status = as_text(obj.get('status') if obj.get('status') else None)
        self._dependency_ids = as_text(obj.get('dependency_ids', '')).split(',')
        self.ttl = int(obj.get('ttl')) if obj.get('ttl') else None
        self.meta = unpickle(obj.get('meta')) if obj.get('meta') else {}
        return self

    def to_dict(self):
        """Returns a serialization of the current job instance"""
        obj = {}
        obj['created_at'] = utcformat(self.created_at or utcnow())
        obj['data'] = self.data

        if self.origin is not None:
            obj['origin'] = self.origin
        if self.description is not None:
            obj['description'] = self.description
        if self.enqueued_at is not None:
            obj['enqueued_at'] = utcformat(self.enqueued_at)
        if self.started_at is not None:
            obj['started_at'] = utcformat(self.started_at)
        if self.ended_at is not None:
            obj['ended_at'] = utcformat(self.ended_at)
        if self._result is not None:
            obj['result'] = dumps(self._result)
        if self.exc_info is not None:
            obj['exc_info'] = self.exc_info
        if self.timeout is not None:
            obj['timeout'] = self.timeout
        if self.result_ttl is not None:
            obj['result_ttl'] = self.result_ttl
        if self._status is not None:
            obj['status'] = self._status
        if self._dependency_ids is not None:
            obj['dependency_ids'] = ','.join(self._dependency_ids)
        if self.meta:
            obj['meta'] = dumps(self.meta)
        if self.ttl:
            obj['ttl'] = self.ttl

        return obj

    def save(self):
        """Persists the current job instance to its corresponding Redis key."""
        key = self.key

        self.redis.hmset(key, self.to_dict())
        self.cleanup(self.ttl)

    def cancel(self):
        """Cancels the given job, which will prevent the job from ever being
        ran (or inspected).

        This method merely exists as a high-level API call to cancel jobs
        without worrying about the internals required to implement job
        cancellation.
        """
        from rq.queue import Queue
        with self.redis.pipeline():
            if self.origin:
                queue = Queue(name=self.origin, connection=self._connection)
                queue.remove(self)

    def delete(self):
        """Cancels the job and deletes the job hash from Redis."""
        self.cancel()
        self.redis.delete(self.key)
        self.redis.delete(self.dependents_key)

    # Job execution
    def perform(self):  # noqa
        """Invokes the job function with the job arguments."""
        self.redis.persist(self.key)
        self.ttl = -1
        self._result = self.func(*self.args, **self.kwargs)
        return self._result

    def get_ttl(self, default_ttl=None):
        """Returns ttl for a job that determines how long a job will be
        persisted. In the future, this method will also be responsible
        for determining ttl for repeated jobs.
        """
        return default_ttl if self.ttl is None else self.ttl

    def get_result_ttl(self, default_ttl=None):
        """Returns ttl for a job that determines how long a jobs result will
        be persisted. In the future, this method will also be responsible
        for determining ttl for repeated jobs.
        """
        return default_ttl if self.result_ttl is None else self.result_ttl

    # Representation
    def get_call_string(self):  # noqa
        """Returns a string representation of the call, formatted as a regular
        Python function invocation statement.
        """
        if self.func_name is None:
            return None

        arg_list = [as_text(repr(arg)) for arg in self.args]

        kwargs = ['{0}={1}'.format(k, as_text(repr(v))) for k, v in self.kwargs.items()]
        # Sort here because python 3.3 & 3.4 makes different call_string
        arg_list += sorted(kwargs)
        args = ', '.join(arg_list)

        return '{0}({1})'.format(self.func_name, args)

    def cleanup(self, ttl=None):
        """Prepare job for eventual deletion (if needed). This method is usually
        called after successful execution. How long we persist the job and its
        result depends on the value of ttl:
        - If ttl is 0, cleanup the job immediately.
        - If it's a positive number, set the job to expire in X seconds.
        - If ttl is negative, don't set an expiry to it (persist
          forever)
        """
        if ttl == 0:
            self.delete()
        elif not ttl:
            return
        elif ttl > 0:
            self.redis.expire(self.key, ttl)

    def __str__(self):
        return '<Job {0}: {1}>'.format(self.id, self.description)

    # Job equality
    def __eq__(self, other):  # noqa
        return isinstance(other, self.__class__) and self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def enqueue_dependents(self):
        """
        Try to enqueue all of the jobs dependents
        """
        for child_id in self._dependency_ids:
            child = Job(child_id, connection=self._connection)
            child.refresh()
            prev_status, new_status = child._try_enqueue_job()

    def requeue_job(self):
        """Requeues the job with the given job ID.  If no such job exists, just
        remove the job ID from the failed queue, otherwise the job ID should refer
        to a failed job (i.e. it should be on the failed queue).
        """
        fq = self._connection.get_failed_queue()
        fq.requeue(self.id)

