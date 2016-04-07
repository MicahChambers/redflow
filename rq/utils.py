# -*- coding: utf-8 -*-
"""
Miscellaneous helper functions.

The formatter for ANSI colored console output is heavily based on Pygments
terminal colorizing code, originally by Georg Brandl.
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import calendar
import datetime
import importlib
import iso8601
import logging
import sys
from collections import Iterable

from rq.compat import is_python_version, string_types
from .keys import (key_for_queue, key_for_job,
                   key_for_deferred_reg, key_for_dependents)


class _Colorizer(object):
    def __init__(self):
        esc = "\x1b["

        self.codes = {}
        self.codes[""] = ""
        self.codes["reset"] = esc + "39;49;00m"

        self.codes["bold"] = esc + "01m"
        self.codes["faint"] = esc + "02m"
        self.codes["standout"] = esc + "03m"
        self.codes["underline"] = esc + "04m"
        self.codes["blink"] = esc + "05m"
        self.codes["overline"] = esc + "06m"

        dark_colors = ["black", "darkred", "darkgreen", "brown", "darkblue",
                       "purple", "teal", "lightgray"]
        light_colors = ["darkgray", "red", "green", "yellow", "blue",
                        "fuchsia", "turquoise", "white"]

        x = 30
        for d, l in zip(dark_colors, light_colors):
            self.codes[d] = esc + "%im" % x
            self.codes[l] = esc + "%i;01m" % x
            x += 1

        del d, l, x

        self.codes["darkteal"] = self.codes["turquoise"]
        self.codes["darkyellow"] = self.codes["brown"]
        self.codes["fuscia"] = self.codes["fuchsia"]
        self.codes["white"] = self.codes["bold"]

        if hasattr(sys.stdout, "isatty"):
            self.notty = not sys.stdout.isatty()
        else:
            self.notty = True

    def reset_color(self):
        return self.codes["reset"]

    def colorize(self, color_key, text):
        if self.notty:
            return text
        else:
            return self.codes[color_key] + text + self.codes["reset"]

    def ansiformat(self, attr, text):
        """
        Format ``text`` with a color and/or some attributes::

            color       normal color
            *color*     bold color
            _color_     underlined color
            +color+     blinking color
        """
        result = []
        if attr[:1] == attr[-1:] == '+':
            result.append(self.codes['blink'])
            attr = attr[1:-1]
        if attr[:1] == attr[-1:] == '*':
            result.append(self.codes['bold'])
            attr = attr[1:-1]
        if attr[:1] == attr[-1:] == '_':
            result.append(self.codes['underline'])
            attr = attr[1:-1]
        result.append(self.codes[attr])
        result.append(text)
        result.append(self.codes['reset'])
        return ''.join(result)


colorizer = _Colorizer()


def make_colorizer(color):
    """Creates a function that colorizes text with the given color.

    For example:

        green = make_colorizer('darkgreen')
        red = make_colorizer('red')

    Then, you can use:

        print "It's either " + green('OK') + ' or ' + red('Oops')
    """
    def inner(text):
        return colorizer.colorize(color, text)
    return inner


class ColorizingStreamHandler(logging.StreamHandler):

    levels = {
        logging.WARNING: make_colorizer('darkyellow'),
        logging.ERROR: make_colorizer('darkred'),
        logging.CRITICAL: make_colorizer('darkred'),
    }

    def __init__(self, exclude=None, *args, **kwargs):
        self.exclude = exclude
        if is_python_version((2, 6)):
            logging.StreamHandler.__init__(self, *args, **kwargs)
        else:
            super(ColorizingStreamHandler, self).__init__(*args, **kwargs)

    @property
    def is_tty(self):
        isatty = getattr(self.stream, 'isatty', None)
        return isatty and isatty()

    def format(self, record):
        message = logging.StreamHandler.format(self, record)
        if self.is_tty:
            colorize = self.levels.get(record.levelno, lambda x: x)

            # Don't colorize any traceback
            parts = message.split('\n', 1)
            parts[0] = " ".join([parts[0].split(" ", 1)[0], colorize(parts[0].split(" ", 1)[1])])

            message = '\n'.join(parts)

        return message


def import_attribute(name):
    """Return an attribute from a dotted path name (e.g. "path.to.func")."""
    module_name, attribute = name.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, attribute)


def utcnow():
    return datetime.datetime.utcnow()


def utcformat(dt=None):
    if dt is None:
        return utcnow().isoformat()
    else:
        return dt.isoformat()


def utcparse(string):
    if string is None:
        return None
    else:
        return iso8601.parse_date(string)


def first(iterable, default=None, key=None):
    """
    Return first element of `iterable` that evaluates true, else return None
    (or an optional default value).

    >>> first([0, False, None, [], (), 42])
    42

    >>> first([0, False, None, [], ()]) is None
    True

    >>> first([0, False, None, [], ()], default='ohai')
    'ohai'

    >>> import re
    >>> m = first(re.match(regex, 'abc') for regex in ['b.*', 'a(.*)'])
    >>> m.group(1)
    'bc'

    The optional `key` argument specifies a one-argument predicate function
    like that used for `filter()`.  The `key` argument, if supplied, must be
    in keyword form.  For example:

    >>> first([1, 1, 3, 4, 5], key=lambda x: x % 2 == 0)
    4

    """
    if key is None:
        for el in iterable:
            if el:
                return el
    else:
        for el in iterable:
            if key(el):
                return el

    return default


def is_nonstring_iterable(obj):
    """Returns whether the obj is an iterable, but not a string"""
    return isinstance(obj, Iterable) and not isinstance(obj, string_types)


def ensure_list(obj):
    """
    When passed an iterable of objects, does nothing, otherwise, it returns
    a list with just that object in it.
    """
    return obj if is_nonstring_iterable(obj) else [obj]


def current_timestamp():
    """Returns current UTC timestamp"""
    return calendar.timegm(datetime.datetime.utcnow().utctimetuple())


def enum(name, *sequential, **named):
    values = dict(zip(sequential, range(len(sequential))), **named)

    # NOTE: Yes, we *really* want to cast using str() here.
    # On Python 2 type() requires a byte string (which is str() on Python 2).
    # On Python 3 it does not matter, so we'll use str(), which acts as
    # a no-op.
    return type(str(name), (), values)


def compact(l):
    return [x for x in l if x is not None]


def _enqueue_or_defer(pipe, job_id, queue_name, dep_status_map):

    from .job import JobStatus

    remaining_deps = [id for id, status in dep_status_map
                      if status != JobStatus.FINISHED]
    if len(remaining_deps) > 0:
        # Dependencies remain, add to deferred list, change status to
        # deferred, clear enqueued at and add job_id to reverse depends
        pipe.sadd(key_for_deferred_reg(queue_name), job_id)
        pipe.hset(key_for_job(job_id), 'status', JobStatus.DEFERRED)
        pipe.hset(key_for_job(job_id), 'enqueued_at', None)

        # Add job_id to the set of jobs to watch by parents
        for dep_id in remaining_deps:
            pipe.sadd(key_for_dependents(dep_id), job_id)
    else:
        # No dependencies, so we are greenlit to enqueue the job. Change its
        # state, set enqueued_at and add to the queue. Remove ourself from
        # reverse deps to be nice as well
        pipe.srem(key_for_deferred_reg(queue_name), job_id)
        pipe.hset(key_for_job(job_id), 'status', JobStatus.QUEUED)
        pipe.hset(key_for_job(job_id), 'enqueued_at', utcformat())
        pipe.rpush(key_for_queue(queue_name), job_id)

        # Add job_id to the set of jobs to watch by parents
        for dep_id in remaining_deps:
            pipe.srem(key_for_dependents(dep_id), job_id)


def _requeue(pipe, job_id, failed_key, origin_name):
    """
    Re-add a job to the
    """
    ##################
    # Querying
    ##################

    # Get all the jobs dependents
    dep_ids = pipe.smembers(key_for_dependents(job_id))
    if dep_ids:
        pipe.watch(*[key_for_job(dep_id) for dep_id in dep_ids])

    # Check that all the dependents have finished
    statuses = {id: pipe.hget(key_for_job(id), 'status')
                for id in dep_ids}

    pipe.multi()

    # Job still exists, so one way or another it is going to be removed
    # form the failed queue
    pipe.lrem(failed_key, job_id)
    pipe.hset(key_for_job(job_id), 'exc_info', None)

    ##################
    # Writing
    ##################
    _enqueue_or_defer(pipe, job_id, origin_name, statuses)


def _attempt_enqueue(pipe, job_id, origin_name):
    # Prepare outputs of previous, final status
    from .job import JobStatus

    status = pipe.hget(key_for_job(job_id), 'status')
    if status != JobStatus.DEFERRED:
        return

    dep_ids = pipe.smembers(key_for_dependents(job_id))
    if dep_ids:
        pipe.watch(*[key_for_job(dep_id) for dep_id in dep_ids])

    # Check that all the dependents have finished
    statuses = {id: pipe.hget(key_for_job(id), 'status')
                for id in dep_ids}

    pipe.multi()

    # write out based on statuses
    _enqueue_or_defer(pipe, job_id, origin_name, statuses)


def _clean_queue(pipe, queue_key):
    """
    Removes all the jobs from a queue. Queue is defined by key rather than name
    since it might be used for the deferred queue as well.
    """
    njobs = pipe.llen(queue_key)
    job_ids = pipe.lrange(queue_key, 0, njobs)
    pipe.multi()
    pipe.delete(queue_key)

    for job_id in job_ids:
        pipe.delete(key_for_job(job_id))
        pipe.delete(key_for_dependents(job_id))

    return len(job_ids)

