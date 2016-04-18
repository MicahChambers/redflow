
QUEUES_KEY = 'rq:queues'
WORKERS_KEY = 'rq:workers'
SUSPENDED_KEY = 'rq:suspended'
REDIS_QUEUE_NAMESPACE_PREFIX = 'rq:queue:'
REDIS_JOB_NAMESPACE_PREFIX = 'rq:job:'

def queue_key_from_name(name):
    return REDIS_QUEUE_NAMESPACE_PREFIX + name

def queue_name_from_key(key):
    assert key.startswith(REDIS_QUEUE_NAMESPACE_PREFIX)
    return key[REDIS_QUEUE_NAMESPACE_PREFIX:]

def job_id_from_key(key):
    assert key.startswith(REDIS_JOB_NAMESPACE_PREFIX)
    return key[REDIS_JOB_NAMESPACE_PREFIX:]

def job_key_from_id(job_id):
    return REDIS_JOB_NAMESPACE_PREFIX + job_id

def children_key_from_id(job_id):
    return 'rq:job:{0}:children'.format(job_id)

def parents_key_from_id(job_id):
    return 'rq:job:{0}:parents'.format(job_id)

def started_registry_key_from_name(name):
    return 'rq:wip:{0}'.format(name)

def finished_registry_key_from_name(name):
    return 'rq:finished:{0}'.format(name)

def deferred_registry_key_from_name(name):
    return 'rq:deferred:{0}'.format(name)

def worker_key_from_name(name):
    return 'rq:worker:{}'.format(name)
