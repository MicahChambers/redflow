
REDIS_QUEUES_KEYS = 'rq:queues'
REDIS_QUEUE_NAMESPACE_PREFIX = 'rq:queue:'

REDIS_JOB_NAMESPACE_PREFIX = 'rq:job:'

def queues_key():
    return REDIS_QUEUES_KEYS

def queue_key(name):
    return REDIS_QUEUE_NAMESPACE_PREFIX + name

def queue_name(key):
    assert key.startswith(REDIS_QUEUE_NAMESPACE_PREFIX)
    return key[REDIS_QUEUE_NAMESPACE_PREFIX:]

def job_id_from_key(key):
    assert key.startswith(REDIS_JOB_NAMESPACE_PREFIX)
    return key[REDIS_JOB_NAMESPACE_PREFIX:]

def job_key_from_id(job_id):
    return REDIS_JOB_NAMESPACE_PREFIX + job_id

def dependents_key_from_id(job_id):
    return 'rq:job:{0}:dependents'.format(job_id)

def dependencies_key_from_id(job_id):
    return 'rq:job:{0}:dependencies'.format(job_id)

def started_registry_key_from_name(name):
    return 'rq:wip:{0}'.format(name)

def finished_registry_key_from_name(name):
    return 'rq:finished:{0}'.format(name)

def deferred_registry_key_from_name(name):
    return 'rq:deferred:{0}'.format(name)
