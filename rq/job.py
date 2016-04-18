# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import inspect
import warnings
from functools import partial
from uuid import uuid4

from rq.compat import as_text, decode_redis_hash, string_types, text_type

from .connections import resolve_connection
from .exceptions import NoSuchJobError, UnpickleError
from .local import LocalStack
from .utils import enum, import_attribute, utcformat, utcnow, utcparse

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


#def cancel_job(job_id, connection=None):
#    """Cancels the job with the given job ID, preventing execution.  Discards
#    any job info (i.e. it can't be requeued later).
#    """
#    Job(job_id, connection=connection).cancel()
#
#
#def requeue_job(job_id, connection=None):
#    """Requeues the job with the given job ID.  If no such job exists, just
#    remove the job ID from the failed queue, otherwise the job ID should refer
#    to a failed job (i.e. it should be on the failed queue).
#    """
#    from .queue import get_failed_queue
#    fq = get_failed_queue(connection=connection)
#    fq.requeue(job_id)
#

def get_current_job(connection=None):
    """Returns the Job instance that is currently being executed.  If this
    function is invoked from outside a job context, None is returned.
    """
    job_id = _job_stack.top
    if job_id is None:
        return None
    return self._storage.get_job(job_id)


class Job(object):
    """
    A Job is just a convenient datastructure to pass around job (meta) data.
    """

    # Job construction
    def _new(self, func, args=None, kwargs=None,
            result_ttl=None, ttl=None, status=None, description=None,
            depends_on=None, timeout=None, id=None, origin=None, meta=None):
        """
        Creates a new Job instance for the given function, arguments, and
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

        self._id = text_type(uuid4())

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
                self._parent_ids = [tmp.id for tmp in depends_on]
            else:
                self._parent_ids = ([depends_on.id]
                                        if isinstance(depends_on, Job)
                                        else [depends_on])

        return job

    @transaction
    def get_status(self):
        self._status = as_text(self._storage._hget(self.key, 'status'))
        return self._status

    @transaction
    def set_status(self, status):
        self._status = status
        self._storage._hset(self.key, 'status', self._status)

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

    @transaction
    @property
    def dependencies(self):
        """
        Returns a list of job's dependencies. To avoid repeated Redis fetches,
        cache job.dependencies
        """
        if self._parent_ids is None:
            return None
        if hasattr(self, '_dependencies'):
            return self._dependencies
        self._dependencies = [self._storage.get_job(dependency_id)
                              for dependency_id in self._parent_ids]

        return self._dependencies

    @transaction
    def remove_dependency(self, dependency_id):
        """
        Removes a dependency from job. This is usually called when
        dependency is successfully executed.
        """
        self._storage._srem(self.dependencies_key, dependency_id)

    @transaction
    def has_unmet_dependencies(self):
        """Checks whether job has dependencies that aren't yet finished."""
        return bool(self._storage._scard(self.parents_key))

    @property
    def dependents(self):
        """
        Returns a list of jobs whose execution depends on this
        job's successful execution"""
        return self.children

    @transaction
    @property
    def children(self):
        """Returns a list of jobs whose execution depends on this
        job's successful execution"""
        children_ids = self._storage._smembers(self.children_key)
        return [self._storage.get_job(id) for id in children_ids]

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

#    @classmethod
#    def exists(cls, job_id, connection=None):
#        """Returns whether a job hash exists for the given job ID."""
#        conn = resolve_connection(connection)
#        return conn.exists(cls.key_for(job_id))
#
#    @classmethod
#    def fetch(cls, id, connection=None):
#        """Fetches a persisted job from its corresponding Redis key and
#        instantiates it.
#        """
#        job = cls(id, connection=connection)
#        job.refresh()
#        return job

    def __init__(self, id=None, connection=None):
        self.connection = resolve_connection(connection)
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
        self._parent_ids = None
        self.meta = {}

    def __repr__(self):  # noqa
        return 'Job({0!r}, enqueued_at={1!r})'.format(self._id, self.enqueued_at)

    # Data access
    @property
    def id(self):
        """
        The job ID for this job instance. Generates an ID lazily the
        first time the ID is requested.
        """
        return self._id

    @property
    def key(self):
        """The Redis key that is used to store job hash under."""
        return job_key_from_id(self.id)

    @property
    def children_key(self):
        """The Redis key that is used to store job dependents hash under."""
        return children_key_from_id(self.id)

    @property
    def dependents_key(self):
        """The Redis key that is used to store job dependents hash under."""
        return self.children_key

    @property
    def dependencies_key(self):
        """The Redis key that is used to store job dependancies hash under."""
        return self.parents_key

    @property
    def parents_key(self):
        """The Redis key that is used to store job dependancies hash under."""
        return parents_key_from_id(self.id)

    @transaction
    @property
    def result(self):
        """Returns the return value of the job.

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
            rv = self._storage._hget(self.key, 'result')
            if rv is not None:
                # cache the result
                self._result = loads(rv)
        return self._result

    # Persistence
    @transaction
    def refresh(self):  # noqa
        """Overwrite the current instance's properties with the values in the
        corresponding Redis key.

        Will raise a NoSuchJobError if no corresponding Redis key exists.
        """
        key = self.key
        obj = decode_redis_hash(self._storage._hgetall(key))
        if len(obj) == 0:
            raise NoSuchJobError('No such job: {0}'.format(key))

        def to_date(date_str):
            if date_str is None:
                return
            else:
                return utcparse(as_text(date_str))

        try:
            self.data = obj['data']
        except KeyError:
            raise NoSuchJobError('Unexpected job format: {0}'.format(obj))

        self.created_at = to_date(as_text(obj.get('created_at')))
        self.origin = as_text(obj.get('origin'))
        self.description = as_text(obj.get('description'))
        self.enqueued_at = to_date(as_text(obj.get('enqueued_at')))
        self.started_at = to_date(as_text(obj.get('started_at')))
        self.ended_at = to_date(as_text(obj.get('ended_at')))
        self._result = unpickle(obj.get('result')) if obj.get('result') else None  # noqa
        self.exc_info = as_text(obj.get('exc_info'))
        self.timeout = int(obj.get('timeout')) if obj.get('timeout') else None
        self.result_ttl = int(obj.get('result_ttl')) if obj.get('result_ttl') else None  # noqa
        self._status = as_text(obj.get('status') if obj.get('status') else None)
        self._parent_ids = as_text(obj.get('parent_ids', '')).split(' ')
        self.ttl = int(obj.get('ttl')) if obj.get('ttl') else None
        self.meta = unpickle(obj.get('meta')) if obj.get('meta') else {}

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
        if self._parent_ids is not None:
            obj['parent_ids'] = ' '.join(self._parent_ids)
        if self.meta:
            obj['meta'] = dumps(self.meta)
        if self.ttl:
            obj['ttl'] = self.ttl

        return obj

    @transaction
    def save(self):
        """Persists the current job instance to its corresponding Redis key."""
        key = self.key

        # TODO also register our dependencies
        self._storage._hmset(key, self.to_dict())
        self.cleanup(self.ttl)

    @transaction
    def cancel(self):
        """
        Cancels the given job, which will prevent the job from ever being
        ran (or inspected).

        This method merely exists as a high-level API call to cancel jobs
        without worrying about the internals required to implement job
        cancellation.
        """
        if self.origin:
            queue = self._storage.mkqueue(self.origin)
            queue.remove(self)

    @transaction
    def delete(self):
        """Cancels the job and deletes the job hash from Redis."""
        self.cancel()
        self._storage._delete(self.key)
        self._storage._delete(self.children_key)
        # TODO remove from queue
        # TODO remove from failed queue
        #self._storage._lrem(queue_key(self.origin), 0, self.id)
        #self._storage._lrem(queue_key(JobStatus.FAILED), 0, self.id)

    # Job execution
    def perform(self):  # noqa
        """
        Invokes the job function with the job arguments.
        """
        self.ttl = -1
        _job_stack.push(self.id)
        try:
            self._result = self.func(*self.args, **self.kwargs)
        finally:
            assert self.id == _job_stack.pop()
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

    @transaction
    def cleanup(self, ttl=None):
        """
        Prepare job for eventual deletion (if needed). This method is usually
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
            self._storage._expire(self.key, ttl)

    @transaction
    def _unfinished_parents(self):
        """
        Intended to be run on in-memory job (i.e. should not load or save from
        the job key itself)

        :return list: of job objects
        """
        self._storage.watch(*[job_key_from_id(id) for id in self._parent_ids])

        unfinished_parents = []
        for parent_id in self._parent_ids:
            parent = self._storage.get_job(parent_id)
            if parent.get_status() != JobStatus.FINISHED:
                unfinished_parents.append(parent)

        return unfinished_parents

    @transaction
    def _add_child(self, child_id):
        """
        Adds a child job id to the list of jobs to update after this job
        finishes
        """
        self._storage._sadd(self.children_key, child_id)

    @transaction
    def _remove_parents(self):
        """
        Adds a child job id to the list of jobs to update after this job
        finishes
        """
        self._storage._delete(self.parents_key)

    @transaction
    def _remove_parent(self, parent_id=None):
        """
        Adds a child job id to the list of jobs to update after this job
        finishes
        """
        self._storage._srem(self.parents_key, parent_id)

    def __str__(self):
        return '<Job {0}: {1}>'.format(self.id, self.description)

    # Job equality
    def __eq__(self, other):  # noqa
        return isinstance(other, self.__class__) and self.id == other.id

    def __hash__(self):
        return hash(self.id)

_job_stack = LocalStack()
