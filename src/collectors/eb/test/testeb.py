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
from eb import BeanstalkCollector

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

class TestBeanstalkCollector(CollectorTestCase):

    def test_throws_exception_when_interval_not_multiple_of_60(self):
        config = get_collector_config('BeanstalkCollector',
                                      {'enabled': True,
                                       'interval': 10})
        assertRaisesAndContains(Exception, 'multiple of',
                                BeanstalkCollector, *[config, None])

    def test_get_default_config(self):
        collector = BeanstalkCollector(None, handlers=[])
        self.assertFalse(collector.config['enabled'])
        self.assertEqual(collector.config['interval'], 60)
        self.assertEqual(collector.config['period'], 300)

    def test_process_config(self):
        config = get_collector_config(
            'BeanstalkCollector',
            {
                'enabled': True,
                'interval': 60,
                'period': 120,
                'regions': {
                    'us-east-1': {
                        'environment_names': ['^test', '^test1$']
                    }
                }
            })
        collector = BeanstalkCollector(config, handlers=[])
        self.assertTrue(collector.config['enabled'])
        self.assertEqual(collector.config['interval'], 60)
        self.assertEqual(collector.config['regions']['us-east-1']['environment_names'], ['^test', '^test1$'])
        self.assertEqual(collector.config['period'], 120)

    def test_get_environment_names_no_regex(self):
        config = get_collector_config(
            'BeanstalkCollector',
            {
                'enabled': True,
                'regions': {
                    'us-east-1': {}
                }
            })
        collector = BeanstalkCollector(config, handlers=[])
        region_eb_client = Mock()
        region_eb_client.describe_environments = Mock()
        region_eb_client.describe_environments.return_value = {
            'EBEnvironments': [
                { 'EnvironmentName' : 'test1' },
                { 'EnvironmentName' : 'test2' },
                { 'EnvironmentName' : 'test3' },
                { 'EnvironmentName' : 'test4' },
            ]
        }
        session = Mock()
        session.client = Mock()
        session.client.return_value = region_eb_client
        environment_names = collector.get_environment_names('us-east-1', session)
        self.assertEqual(environment_names, ['test1','test2','test3','test4'])

    def test_get_environment_names_regex(self):
        config = get_collector_config(
            'BeanstalkCollector',
            {
                'enabled': True,
                'regions': {
                    'us-east-1': {
                        'environment_names': ['^test1$', 'zyx']
                    }
                }
            })
        collector = BeanstalkCollector(config, handlers=[])
        region_eb_client = Mock()
        region_eb_client.describe_environments = Mock()
        region_eb_client.describe_environments.return_value = {
            'EBEnvironments': [
                { 'EnvironmentName' : 'test1' },
                { 'EnvironmentName' : 'test2' },
                { 'EnvironmentName' : 'test3' },
                { 'EnvironmentName' : 'test4' },
                { 'EnvironmentName' : 'zyx89' },
            ]
        }
        session = Mock()
        session.client = Mock()
        session.client.return_value = region_eb_client
        environment_names = collector.get_environment_names('us-east-1', session)
        self.assertEquals(environment_names, ['test1','zyx89'])

    @patch.object(Collector, 'publish_metric')
    def test_process_stat(self, publish_metric):
        config = get_collector_config(
            'BeanstalkCollector',
            {
                'enabled': True,
                'regions': {
                    'us-east-1': {
                        'environment_names': ['^test1$', 'zyx']
                    }
                }
            })
        metric = BeanstalkCollector.MetricInfo('InstancesOk', 'Average', 'GAUGE', 0, False)
        stat = {
            'Average': 40,
            'Timestamp': datetime.datetime.utcnow()
        }
        collector = BeanstalkCollector(config, handlers=[])
        collector.process_stat('us-east-1', 'test1', metric, stat)
        self.assertPublishedMetricMany(
            publish_metric,
            {
                'us-east-1.test1.InstancesOk': 40
            }
        )
