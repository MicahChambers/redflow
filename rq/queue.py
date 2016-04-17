# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import uuid

from redis import WatchError

from .compat import as_text, string_types, total_ordering
from .connections import resolve_connection
from .defaults import DEFAULT_RESULT_TTL
from .exceptions import (DequeueTimeout, InvalidJobOperationError,
                         NoSuchJobError, UnpickleError)
from .job import Job, JobStatus
from .utils import import_attribute, utcnow


def get_failed_queue(connection=None):
    """Returns a handle to the special failed queue."""
    return FailedQueue(connection=connection)


def compact(lst):
    return [item for item in lst if item is not None]


@total_ordering
class Queue(object):
    job_class = Job
    DEFAULT_TIMEOUT = 180  # Default timeout seconds.

    def __init__(self, name='default', default_timeout=None, async=True,
                 job_class=None, storage=None):
        self._storage = storage
        self.name = name
        self._key = queue_key(name)
        self._default_timeout = default_timeout
        self._async = async

        if job_class is not None:
            if isinstance(job_class, string_types):
                job_class = import_attribute(job_class)
            self.job_class = job_class

    def __len__(self):
        return self.count

    def __iter__(self):
        yield self

    @property
    def key(self):
        """Returns the Redis key for this Queue."""
        return self._key

    @transaction
    def empty(self):
        job_ids = self._storage._lrange(self.key, 0, -1)
        for job_id in job_ids:
            job = self._storage.get_job(job_id)
            job.delete()
        return len(job_ids)

    def is_empty(self):
        """Returns whether the current queue is empty."""
        return self.count == 0

    @transaction
    def get_job_ids(self, offset=0, length=-1):
        """Returns a slice of job IDs in the queue."""
        start = offset
        if length >= 0:
            end = offset + (length - 1)
        else:
            end = length
        return self._storage._lrange(self.key, start, end)

    @transaction
    def get_jobs(self, offset=0, length=-1):
        """Returns a slice of jobs in the queue."""
        job_ids = self.get_job_ids(offset, length)
        return compact([self._storage.get_job(job_id) for job_id in job_ids])

    @property
    def job_ids(self):
        """Returns a list of all job IDS in the queue."""
        return self.get_job_ids()

    @property
    def jobs(self):
        """Returns a list of all (valid) jobs in the queue."""
        return self.get_jobs()

    @transaction
    @property
    def count(self):
        """Returns a count of all messages in the queue."""
        return self._storage._llen(self.key)

    @transaction
    def remove(self, job_id):
        """Removes Job from queue, accepts either a Job instance or ID."""
        return self._storage._lrem(self.key, 1, job_id)

    @transaction
    def compact(self):
        """
        Removes all "dead" jobs from the queue by cycling through it, while
        guaranteeing FIFO semantics.
        """
        job_ids = self.get_job_ids(offset, length)
        jobs = {job_id : self._storage.get_job(job_id) for job_id in job_ids]}

        to_keep = [job_id for job_id, job in jobs.items() if job is not None]
        self._storage._delete(self.key)
        self._storage._lpush(to_keep)

    @transaction
    def push_job_id(self, job_id, at_front=False):
        """
        Pushes a job ID on the corresponding Redis queue.
        'at_front' allows you to push the job onto the front instead of the back
        of the queue
        """
        if at_front:
            self._storage._lpush(self.key, job_id)
        else:
            self._storage._rpush(self.key, job_id)

    @transaction
    def _enqueue_job(self, func, args=None, kwargs=None, timeout=None,
                     result_ttl=None, ttl=None, description=None,
                     depends_on=None, job_id=None, at_front=False, meta=None):

        timeout = timeout or self._default_timeout

        # Load existing, or create a new job
        if job_id is not None:
            job = self.job_class(job_id, storage=self._storage)
            job.refresh()
        else:
            # Create a new job (but don't write until the end)
            job = self.job_class(storage=self._storage)
            job._new(storage=self._storage, func, args=args, kwargs=kwargs,
                     result_ttl=result_ttl, ttl=ttl, status=JobStatus.QUEUED,
                     description=description, depends_on=depends_on,
                     timeout=timeout, id=job_id, origin=self.name, meta=meta)

        # If job depends on an unfinished job, register itself on it's
        # parent's dependents instead of enqueueing it.
        # If WatchError is raised in the process, that means something else is
        # modifying the dependency. In this case we simply retry
        remaining_dependencies = []
        if depends_on:
            if not isinstance(depends_on, list):
                if not isinstance(depends_on, self.job_class):
                    depends_on = Job(id=depends_on, storage=self._storage)
                dependencies = [depends_on]
            else:
                dependencies = depends_on

            pipe.watch(*[dependency.key for dependency in dependencies])
            for dependency in dependencies:
                if dependency.get_status() != JobStatus.FINISHED:
                    remaining_dependencies.append(dependency)

        # Writing
        if remaining_dependencies:
            job.set_status(JobStatus.DEFERRED)
            job.register_dependencies(remaining_dependencies)
            job.save()
            return job
        else:
            # Add Queue key set
            self._storage._sadd(queues_key(self.name), self.key)
            job.set_status(JobStatus.QUEUED)

            job.origin = self.name
            job.enqueued_at = utcnow()

            if job.timeout is None:
                job.timeout = self.DEFAULT_TIMEOUT
            job.save()

            if self._async:
                self.push_job_id(job.id, at_front=at_front)

            return job


    def enqueue_call(self, func, args=None, kwargs=None, timeout=None,
                     result_ttl=None, ttl=None, description=None,
                     depends_on=None, job_id=None, at_front=False, meta=None):
        """
        Creates a job to represent the delayed function call and enqueues
        it.

        It is much like `.enqueue()`, except that it takes the function's args
        and kwargs as explicit arguments.  Any kwargs passed to this function
        contain options for RQ itself.
        """
        self._enqueue_new_job()

        if not self._async:
            job.perform()
            job._save_results()

        return job

    def enqueue(self, f, *args, **kwargs):
        """Creates a job to represent the delayed function call and enqueues
        it.

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
            assert args == (), 'Extra positional arguments cannot be used '
                'when using explicit args and kwargs'
            args = kwargs.pop('args', None)
            kwargs = kwargs.pop('kwargs', None)

        return self.enqueue_call(func=f, args=args, kwargs=kwargs,
                                 timeout=timeout, result_ttl=result_ttl, ttl=ttl,
                                 description=description, depends_on=depends_on,
                                 job_id=job_id, at_front=at_front, meta=meta)

    @transaction
    def enqueue_dependents(self, job):
        """Enqueues all jobs in the given job's dependents set and clears it."""
        from .registry import DeferredJobRegistry

        job_ids = self._storage.smembers(job.dependents_key)
        dependents = [self._storage.get_job(job_id) for job_id in job_ids]
        registries = [self._storage.get_deferred_registery(job.origin)
                      for job in dependents]

        for dependent, registry in zip(dependents, registries):
            if not dependent.has_unmet_dependencies():
                if dependent.origin == self.name:
                    self.enqueue_job(dependent, pipeline=pipeline)
                else:
                    queue = Queue(name=dependent.origin,
                                  connection=self.connection)
                    queue.enqueue_job(dependent, pipeline=pipeline)
            pipeline.execute()

        # writes
        for dependent, registry in zip(dependents, registries):
            registry.remove(dependent)
            dependent.remove_dependency(job.id)

    def enqueue_dependents(self, job):
        """Enqueues all jobs in the given job's dependents set and clears it."""
        from .registry import DeferredJobRegistry

        while True:
            job_id = as_text(self.connection.spop(job.dependents_key))
            if job_id is None:
                break
            dependent = self.job_class.fetch(job_id, connection=self.connection)
            registry = DeferredJobRegistry(dependent.origin, self.connection)
            with self.connection._pipeline() as pipeline:
                registry.remove(dependent, pipeline=pipeline)
                dependent.remove_dependency(job.id)
                if not dependent.has_unmet_dependencies():
                    if dependent.origin == self.name:
                        self.enqueue_job(dependent, pipeline=pipeline)
                    else:
                        queue = Queue(name=dependent.origin,
                                      connection=self.connection)
                        queue.enqueue_job(dependent, pipeline=pipeline)
                pipeline.execute()

    def pop_job_id(self):
        """Pops a given job ID from this Redis queue."""
        return as_text(self.connection.lpop(self.key))

    @classmethod
    def lpop(cls, queue_keys, timeout, connection=None):
        """Helper method.  Intermediate method to abstract away from some
        Redis API details, where LPOP accepts only a single key, whereas BLPOP
        accepts multiple.  So if we want the non-blocking LPOP, we need to
        iterate over all queues, do individual LPOPs, and return the result.

        Until Redis receives a specific method for this, we'll have to wrap it
        this way.

        The timeout parameter is interpreted as follows:
            None - non-blocking (return immediately)
             > 0 - maximum number of seconds to block
        """
        connection = resolve_connection(connection)
        if timeout is not None:  # blocking variant
            if timeout == 0:
                raise ValueError('RQ does not support indefinite timeouts. Please pick a timeout value > 0')
            result = connection.blpop(queue_keys, timeout)
            if result is None:
                raise DequeueTimeout(timeout, queue_keys)
            queue_key, job_id = result
            return queue_key, job_id
        else:  # non-blocking variant
            for queue_key in queue_keys:
                blob = connection.lpop(queue_key)
                if blob is not None:
                    return queue_key, blob
            return None

    def dequeue(self):
        """Dequeues the front-most job from this queue.

        Returns a job_class instance, which can be executed or inspected.
        """
        while True:
            job_id = self.pop_job_id()
            if job_id is None:
                return None
            try:
                job = self.job_class.fetch(job_id, connection=self.connection)
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

    @classmethod
    def dequeue_any(cls, queues, timeout, connection=None):
        """Class method returning the job_class instance at the front of the given
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

    def clean_registries(self):
        """Cleans StartedJobRegistry and FinishedJobRegistry of a queue."""
        registry = FinishedJobRegistry(name=self.name, storage=self._storage)
        registry.cleanup()
        registry = StartedJobRegistry(name=self.name, storage=self._storage)
        registry.cleanup()


class FailedQueue(Queue):
    def __init__(self, connection=None):
        super(FailedQueue, self).__init__(JobStatus.FAILED, connection=connection)

    def quarantine(self, job, exc_info):
        """Puts the given Job in quarantine (i.e. put it on the failed
        queue).
        """

        with self.connection._pipeline() as pipeline:
            # Add Queue key set
            self.connection.sadd(self.redis_queues_keys, self.key)

            job.ended_at = utcnow()
            job.exc_info = exc_info
            job.save(pipeline=pipeline)

            self.push_job_id(job.id, pipeline=pipeline)
            pipeline.execute()

        return job

    def requeue(self, job_id):
        """Requeues the job with the given job ID."""
        try:
            job = self.job_class.fetch(job_id, connection=self.connection)
        except NoSuchJobError:
            # Silently ignore/remove this job and return (i.e. do nothing)
            self.remove(job_id)
            return

        # Delete it from the failed queue (raise an error if that failed)
        if self.remove(job) == 0:
            raise InvalidJobOperationError('Cannot requeue non-failed jobs')

        job.set_status(JobStatus.QUEUED)
        job.exc_info = None
        q = Queue(job.origin, connection=self.connection)
        q.enqueue_job(job)
