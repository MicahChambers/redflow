# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from rq import RQConnection, Queue

from tests import find_empty_redis_database, RQTestCase
from tests.fixtures import do_nothing


def new_connection():
    return find_empty_redis_database()


class TestConnectionInheritance(RQTestCase):

    def test_init(self):
        """ Pinging redis server should work """
        conn = RQConnection(new_connection())
        self.assertTrue(conn.ping())

    def test_get_no_queues(self):
        """ Test getting queues when none are there """
        conn = RQConnection(new_connection())
        self.assertEqual(conn.get_queues(), [])

    def test_get_no_workers(self):
        """ Test getting workers when none are there """
        conn = RQConnection(new_connection())
        self.assertEqual(conn.get_workers(), [])
