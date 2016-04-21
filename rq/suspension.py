from rq.utils import transaction

WORKERS_SUSPENDED = 'rq:suspended'

@transaction
def is_suspended(conn):
    return conn._exists(WORKERS_SUSPENDED)

@transaction
def suspend(conn, ttl=None):
    """
    :param conn:
    :param ttl: time to live in seconds.  Default is no expiration
           Note: If you pass in 0 it will invalidate right away
    """
    conn._set(WORKERS_SUSPENDED, 1)
    if ttl is not None:
        conn_.expire(WORKERS_SUSPENDED, ttl)

@transaction
def resume(conn):
    return conn._delete(WORKERS_SUSPENDED)

