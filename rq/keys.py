"""
Helper functions for resolving redis keys to names, and vice-versa.
"""

REDIS_WORKER_NAMESPACE_PREFIX = 'rq:worker:'
REDIS_WORKERS_KEY = 'rq:workers'
REDIS_QUEUE_NAMESPACE_PREFIX = 'rq:queue:'
REDIS_QUEUES_KEY = 'rq:queues'

def key_for_dependents(job_id):
    """ Get the key for the set of job's dependents """
    return b'rq:job:{}:dependents'.format(job_id)

def key_for_job(job_id):
    """The Redis key that is used to store job hash under."""
    return b'rq:job:' + job_id.encode('utf-8')


def dependents_key_for_job(job_id):
    """The Redis key that is used to store job dependents hash under."""
    return 'rq:job:{0}:dependents'.format(job_id)


def queue_key_to_name(queue_key):
    """Returns a Queue instance, based on the naming conventions for naming
    the internal Redis keys.  Can be used to reverse-lookup Queues by their
    Redis keys.
    """
    prefix = REDIS_QUEUE_NAMESPACE_PREFIX
    if not queue_key.startswith(prefix):
        raise ValueError('Not a valid RQ queue key: {0}'.format(queue_key))
    return queue_key[len(prefix):]

def deferred_registry_name_to_key(name='default'):
    return 'rq:deferred:{0}'.format(name)

def queue_name_to_key(name):
    """Returns a Queue instance, based on the naming conventions for naming
    the internal Redis keys.  Can be used to reverse-lookup Queues by their
    Redis keys.
    """
    prefix = REDIS_QUEUE_NAMESPACE_PREFIX
    return '{0}{1}'.format(prefix, name)

def worker_name_to_key(worker_name):
    return REDIS_WORKER_NAMESPACE_PREFIX + worker_name

def worker_key_to_name(worker_key):
    assert worker_key.startswith(REDIS_QUEUE_NAMESPACE_PREFIX)
    return worker_key[len(REDIS_WORKER_NAMESPACE_PREFIX):]
