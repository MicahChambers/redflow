# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import uuid

from redis import WatchError

from rq.compat import as_text, string_types, total_ordering
from rq.defaults import DEFAULT_RESULT_TTL
from rq.exceptions import (InvalidJobOperationError, NoSuchJobError, UnpickleError)
from rq.job import Job, JobStatus
from rq.utils import import_attribute, utcnow
from rq.keys import queue_name_to_key, queue_key_to_name, REDIS_QUEUES_KEY

def compact(lst):
    return [item for item in lst if item is not None]


@total_ordering
class Queue(object):
    DEFAULT_TIMEOUT = 180  # Default timeout seconds.

    def __init__(self, name='default', default_timeout=None, async=True,
                 connection=None):

        if isinstance(connection, RQConnection):
            self._connection = connection
        else:
            self._connection = RQConnection(connection)

        self.name = name
        self._default_timeout = default_timeout
        self._async = async
        self._key = queue_name_to_key(name)

    def __len__(self):
        return self.count

    def __iter__(self):
        yield self

    @property
    def key(self):
        """Returns the Redis key for this Queue."""
        return self._key

    def empty(self):
        script = b"""
            local prefix = "rq:job:"
            local q = KEYS[1]
            local count = 0
            while true do
                local job_id = redis.call("lpop", q)
                if job_id == false then
                    break
                end

                -- Delete the relevant keys
                redis.call("del", prefix..job_id)
                redis.call("del", prefix..job_id..":dependents")
                count = count + 1
            end
            return count
        """
        script = self.connection._register_script(script)
        return script(keys=[self.key])

    def is_empty(self):
        """Returns whether the current queue is empty."""
        return self.count == 0

    def get_job(self, job_id):
        return self._connection.get_job(job_id)

    def get_job_ids(self, offset=0, length=-1):
        """Returns a slice of job IDs in the queue."""
        start = offset
        if length >= 0:
            end = offset + (length - 1)
        else:
            end = length
        return [as_text(job_id) for job_id in
                self._connection._lrange(self.key, start, end)]

    def get_jobs(self, offset=0, length=-1):
        """Returns a slice of jobs in the queue."""
        job_ids = self.get_job_ids(offset, length)
        return compact([self._connection.get_job(job_id) for job_id in job_ids])

    @property
    def job_ids(self):
        """Returns a list of all job IDS in the queue."""
        return self.get_job_ids()

    @property
    def jobs(self):
        """Returns a list of all (valid) jobs in the queue."""
        return self.get_jobs()

    @property
    def count(self):
        """Returns a count of all messages in the queue."""
        return self._connection._llen(self.key)

    def remove(self, job_or_id):
        """Removes Job from queue, accepts either a Job instance or ID."""
        job_id = job_or_id.id if isinstance(job_or_id, Job) else job_or_id

        return self._connection._lrem(self.key, 1, job_id)

    def compact(self):
        """
        Removes all "dead" jobs from the queue by cycling through it, while
        guaranteeing FIFO semantics.
        """
        COMPACT_QUEUE = 'rq:queue:_compact:{0}'.format(uuid.uuid4())

        self._connection._rename(self.key, COMPACT_QUEUE)
        while True:
            job_id = as_text(self._connection._lpop(COMPACT_QUEUE))
            if job_id is None:
                break
            if self._connection.get_job(job_id) is not None:
                self._connection._rpush(self.key, job_id)

    def push_job_id(self, job_id, at_front=False):
        """Pushes a job ID on the corresponding Redis queue.
        'at_front' allows you to push the job onto the front instead of the back of the queue"""
        if at_front:
            self._connection._lpush(self.key, job_id)
        else:
            self._connection._rpush(self.key, job_id)

    ## TODO HERE
    def enqueue_call(self, func, args=None, kwargs=None, timeout=None,
                     result_ttl=None, ttl=None, description=None,
                     depends_on=None, job_id=None, at_front=False, meta=None):
        """
        Creates a job to represent the delayed function call and enqueues it.

        It is much like `.enqueue()`, except that it takes the function's args
        and kwargs as explicit arguments.  Any kwargs passed to this function
        contain options for RQ itself.
        """
        timeout = timeout or self._default_timeout

        # All jobs should start as deferred, Note: the definitive job status is
        # the status field, not whether the job is in the deferred registry
        job = Job(connection=self.connection)
        job.create(func, args=args, kwargs=kwargs, connection=self._connection,
                   result_ttl=result_ttl, ttl=ttl, status=JobStatus.DEFERRED,
                   description=description, depends_on=depends_on,
                   timeout=timeout, id=job_id, origin=self.name, meta=meta)
        job.save()

        self._connection._sadd(REDIS_QUEUES_KEY, self.key)
        job._attempt_enqueue()
        return job

    def enqueue(self, f, *args, **kwargs):
        """
        Creates a job to represent the delayed function call and enqueues it.

        Expects the function to call, along with the arguments and keyword
        arguments.

        The function argument `f` may be any of the following:

        * A reference to a function
        * A reference to an object's instance method
        * A string, representing the location of a function (must be
          meaningful to the import context of the workers)
        """
        if not isinstance(f, string_types) and f.__module__ == '__main__':
            raise ValueError('Functions from the __main__ module cannot be processed '
                             'by workers')

        # Detect explicit invocations, i.e. of the form:
        #     q.enqueue(foo, args=(1, 2), kwargs={'a': 1}, timeout=30)
        timeout = kwargs.pop('timeout', None)
        description = kwargs.pop('description', None)
        result_ttl = kwargs.pop('result_ttl', None)
        ttl = kwargs.pop('ttl', None)
        depends_on = kwargs.pop('depends_on', None)
        job_id = kwargs.pop('job_id', None)
        at_front = kwargs.pop('at_front', False)
        meta = kwargs.pop('meta', None)

        if 'args' in kwargs or 'kwargs' in kwargs:
            assert args == (), 'Extra positional arguments cannot be used ' \
                    'when using explicit args and kwargs'
            args = kwargs.pop('args', None)
            kwargs = kwargs.pop('kwargs', None)

        return self.enqueue_call(func=f, args=args, kwargs=kwargs,
                                 timeout=timeout, result_ttl=result_ttl, ttl=ttl,
                                 description=description, depends_on=depends_on,
                                 job_id=job_id, at_front=at_front, meta=meta)

    def pop_job_id(self):
        """Pops a given job ID from this Redis queue."""
        return as_text(self._connection.lpop(self.key))

    def dequeue(self):
        """Dequeues the front-most job from this queue.

        Returns a Job instance, which can be executed or inspected.
        """
        while True:
            job_id = self.pop_job_id()
            if job_id is None:
                return None
            try:
                job = Job.fetch(job_id, connection=self._connection)
            except NoSuchJobError as e:
                # Silently pass on jobs that don't exist (anymore),
                continue
            except UnpickleError as e:
                # Attach queue information on the exception for improved error
                # reporting
                e.job_id = job_id
                e.queue = self
                raise e
            return job

    # Total ordering defition (the rest of the required Python methods are
    # auto-generated by the @total_ordering decorator)
    def __eq__(self, other):  # noqa
        if not isinstance(other, Queue):
            raise TypeError('Cannot compare queues to other objects')
        return self.name == other.name

    def __lt__(self, other):
        if not isinstance(other, Queue):
            raise TypeError('Cannot compare queues to other objects')
        return self.name < other.name

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):  # noqa
        return 'Queue({0!r})'.format(self.name)

    def __str__(self):
        return '<Queue {0!r}>'.format(self.name)


class FailedQueue(Queue):
    def __init__(self, connection=None):
        super(FailedQueue, self).__init__(JobStatus.FAILED, connection=connection)

    def quarantine(self, job, exc_info):
        """Puts the given Job in quarantine (i.e. put it on the failed
        queue).
        """

        with self._connection._pipeline() as pipeline:
            # Add Queue key set
            self._connection.sadd(REDIS_QUEUES_KEY, self.key)

            job.ended_at = utcnow()
            job.exc_info = exc_info
            job.save(pipeline=pipeline)

            self.push_job_id(job.id, pipeline=pipeline)
            pipeline.execute()

        return job

    def requeue(self, job_id):
        """Requeues the job with the given job ID."""
        try:
            job = Job.fetch(job_id, connection=self._connection)
        except NoSuchJobError:
            # Silently ignore/remove this job and return (i.e. do nothing)
            self.remove(job_id)
            return

        # Delete it from the failed queue (raise an error if that failed)
        if self.remove(job) == 0:
            raise InvalidJobOperationError('Cannot requeue non-failed jobs')

        job.set_status(JobStatus.QUEUED)
        job.exc_info = None
        q = Queue(job.origin, connection=self._connection)
        q.enqueue_job(job)

