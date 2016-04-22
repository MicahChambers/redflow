# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from functools import wraps

from rq.compat import string_types

from .defaults import DEFAULT_RESULT_TTL
from .queue import Queue
from .connections import get_connection

class job(object):
    def __init__(self, queue, timeout=None, result_ttl=DEFAULT_RESULT_TTL,
                 ttl=None, connection_name='default'):
        """A decorator that adds a ``delay`` method to the decorated function,
        which in turn creates a RQ job when called. Accepts a required
        ``queue_name`` argument that can be either a ``Queue`` instance or a
        string denoting the queue name.  For example:

            @job(queue='default')
            def simple_add(x, y):
                return x + y

            simple_add.delay(1, 2) # Puts simple_add function into queue
        """
        self.queue = queue
        self.connection_name = connection_name
        self.timeout = timeout
        self.result_ttl = result_ttl
        self.ttl = ttl

    def __call__(self, f):
        @wraps(f)
        def delay(*args, **kwargs):

            if isinstance(self.queue, Queue):
                queue = self.queue
            else:
                connection = get_connection(self.connection_name)
                queue = Queue(name=self.queue, storage=connection)

            depends_on = kwargs.pop('depends_on', None)
            return queue.enqueue_call(f, args=args, kwargs=kwargs,
                                      timeout=self.timeout, result_ttl=self.result_ttl,
                                      ttl=self.ttl, depends_on=depends_on)
        f.delay = delay
        return f

