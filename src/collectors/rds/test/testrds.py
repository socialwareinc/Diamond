#!/usr/bin/python
# coding=utf-8

import datetime
import mock

from test import CollectorTestCase
from test import get_collector_config
from mock import Mock
from mock import patch
from test import run_only

from diamond.collector import Collector
from rds import RdsCollector

def assertRaisesAndContains(excClass, contains_str, callableObj, *args,
                            **kwargs):
    try:
        callableObj(*args, **kwargs)
    except excClass, e:
        msg = str(e)
        if contains_str in msg:
            return
        else:
            raise AssertionError(
                "Exception message does not contain '%s': '%s'" % (
                    contains_str, msg))
    else:
        if hasattr(excClass, '__name__'):
            excName = excClass.__name__
        else:
            excName = str(excClass)
        raise AssertionError("%s not raised" % excName)

class TestRdsCollector(CollectorTestCase):

    def test_throws_exception_when_interval_not_multiple_of_60(self):
        config = get_collector_config('RdsCollector',
                                      {'enabled': True,
                                       'interval': 10})
        assertRaisesAndContains(Exception, 'multiple of',
                                RdsCollector, *[config, None])

    def test_get_default_config(self):
        collector = RdsCollector(None, handlers=[])
        self.assertFalse(collector.config['enabled'])
        self.assertEqual(collector.config['interval'], 60)
        self.assertEqual(collector.config['period'], 300)

    def test_process_config(self):
        config = get_collector_config(
            'RdsCollector',
            {
                'enabled': True,
                'interval': 60,
                'period': 120,
                'regions': {
                    'us-east-1': {
                        'rds_ids': ['^test', '^test1$']
                    }
                }
            })
        collector = RdsCollector(config, handlers=[])
        self.assertTrue(collector.config['enabled'])
        self.assertEqual(collector.config['interval'], 60)
        self.assertEqual(collector.config['regions']['us-east-1']['rds_ids'], ['^test', '^test1$'])
        self.assertEqual(collector.config['period'], 120)

    def test_get_rds_identifiers_no_regex(self):
        config = get_collector_config(
            'RdsCollector',
            {
                'enabled': True,
                'regions': {
                    'us-east-1': {}
                }
            })
        collector = RdsCollector(config, handlers=[])
        region_rds_client = Mock()
        region_rds_client.describe_db_instances = Mock()
        region_rds_client.describe_db_instances.return_value = {
            'DBInstances': [
                { 'DBInstanceIdentifier' : 'test1' },
                { 'DBInstanceIdentifier' : 'test2' },
                { 'DBInstanceIdentifier' : 'test3' },
                { 'DBInstanceIdentifier' : 'test4' },
            ]
        }
        session = Mock()
        session.client = Mock()
        session.client.return_value = region_rds_client
        rds_ids = collector.get_rds_identifiers('us-east-1', session)
        self.assertEqual(rds_ids, ['test1','test2','test3','test4'])

    def test_get_rds_identifiers_regex(self):
        config = get_collector_config(
            'RdsCollector',
            {
                'enabled': True,
                'regions': {
                    'us-east-1': {
                        'rds_ids': ['^test1$', 'zyx']
                    }
                }
            })
        collector = RdsCollector(config, handlers=[])
        region_rds_client = Mock()
        region_rds_client.describe_db_instances = Mock()
        region_rds_client.describe_db_instances.return_value = {
            'DBInstances': [
                { 'DBInstanceIdentifier' : 'test1' },
                { 'DBInstanceIdentifier' : 'test2' },
                { 'DBInstanceIdentifier' : 'test3' },
                { 'DBInstanceIdentifier' : 'test4' },
                { 'DBInstanceIdentifier' : 'zyx89' },
            ]
        }
        session = Mock()
        session.client = Mock()
        session.client.return_value = region_rds_client
        rds_ids = collector.get_rds_identifiers('us-east-1', session)
        self.assertEquals(rds_ids, ['test1','zyx89'])

    @patch.object(Collector, 'publish_metric')
    def test_process_stat(self, publish_metric):
        config = get_collector_config(
            'RdsCollector',
            {
                'enabled': True,
                'regions': {
                    'us-east-1': {
                        'rds_ids': ['^test1$', 'zyx']
                    }
                }
            })
        metric = RdsCollector.MetricInfo('CPUUtilization', 'Average', 'GAUGE', 2, False)
        stat = {
            'Average': 40,
            'Timestamp': datetime.datetime.utcnow()
        }
        collector = RdsCollector(config, handlers=[])
        collector.process_stat('us-east-1', 'test1', metric, stat)
        self.assertPublishedMetricMany(
            publish_metric,
            {
                'us-east-1.test1.CPUUtilization': 40
            }
        )
