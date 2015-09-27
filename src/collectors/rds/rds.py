# coding=utf-8

"""
The RDS collector collects metrics for one or more AWS RDS instances

#### Configuration

Below is an example configuration for the RdsCollector.
You can specify an arbitrary amount of regions.

```
    enabled = true
    # must be a multiple of 60
    interval = 60

    # Optional
    access_key_id = ...
    secret_access_key = ...

    [regions]

    [[us-west-1]]
    # Optional - queries all rds instances if omitted, supports regex
    rds_ids = db-1, db-test-*

    [[us-west-2]]
    ...
```

#### Dependencies

 * boto3

"""
import calendar
import datetime
import re
import time
import threading
from collections import namedtuple
from string import Template

import diamond.collector
from diamond.collector import str_to_bool
from diamond.metric import Metric

try:
    import boto3
    from boto3.session import Session
except ImportError:
    boto3 = False

class memoized(object):
    """Decorator that caches a function's return value each time it is called.
    If called later with the same arguments, the cached value is returned, and
    the function is not re-evaluated.

    Based upon from http://wiki.python.org/moin/PythonDecoratorLibrary#Memoize
    Nota bene: this decorator memoizes /all/ calls to the function.  For
    a memoization decorator with limited cache size, consider:
    bit.ly/1wtHmlM
    """

    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __call__(self, *args, **kwargs):
        # If the function args cannot be used as a cache hash key, fail fast
        key = cPickle.dumps((args, kwargs))
        try:
            return self.cache[key]
        except KeyError:
            value = self.func(*args, **kwargs)
            self.cache[key] = value
            return value

    def __repr__(self):
        """Return the function's docstring."""
        return self.func.__doc__

    def __get__(self, obj, objtype):
        """Support instance methods."""
        return functools.partial(self.__call__, obj)


def utc_to_local(utc_dt):
    """
    :param utc_dt: datetime in UTC
    :return: datetime in the local timezone
    """
    # get integer timestamp to avoid precision lost
    timestamp = calendar.timegm(utc_dt.timetuple())
    local_dt = datetime.datetime.fromtimestamp(timestamp)
    assert utc_dt.resolution >= datetime.timedelta(microseconds=1)
    return local_dt.replace(microsecond=utc_dt.microsecond)

class RdsCollector(diamond.collector.Collector):

    # default_to_zero means if cloudwatch does not return a stat for the
    # given metric, then just default it to zero.
    MetricInfo = namedtuple(
        'MetricInfo',
        'name aws_type diamond_type precision default_to_zero')

    # AWS metrics for ELBs
    metrics = [
        MetricInfo('BinLogDiskUsage', 'Average', 'GAUGE', 0, False),
        MetricInfo('CPUUtilization', 'Average', 'GAUGE', 2, False),
        MetricInfo('CPUCreditUsage', 'Average', 'GAUGE', 0, False),
        MetricInfo('CPUCreditBalance', 'Average', 'GAUGE', 0, False),
        MetricInfo('DatabaseConnections', 'Average', 'GAUGE', 0, True),
        MetricInfo('DiskQueueDepth', 'Average', 'GAUGE', 0, True),
        MetricInfo('FreeableMemory', 'Average', 'GAUGE', 0, True),
        MetricInfo('FreeStorageSpace', 'Average', 'GAUGE', 0, True),
        MetricInfo('ReplicaLag', 'Average', 'GAUGE', 3, True),
        MetricInfo('SwapUsage', 'Average', 'GAUGE', 0, True),
        MetricInfo('ReadIOPS', 'Average', 'GAUGE', 5, True),
        MetricInfo('WriteIOPS', 'Average', 'GAUGE', 5, True),
        MetricInfo('ReadLatency', 'Average', 'GAUGE', 5, True),
        MetricInfo('WriteLatency', 'Average', 'GAUGE', 5, True),
        MetricInfo('ReadThroughput', 'Average', 'GAUGE', 5, True),
        MetricInfo('WriteThroughput', 'Average', 'GAUGE', 5, True),
        MetricInfo('NetworkReceiveThroughput', 'Average', 'GAUGE', 5, True),
        MetricInfo('NetworkTransmitThroughput', 'Average', 'GAUGE', 5, True)
    ]

    def process_config(self):
        super(RdsCollector, self).process_config()
        if str_to_bool(self.config['enabled']):
            self.interval = self.config.as_int('interval')
            self.period = self.config.as_int('period')
            # CloudWatch only supports intervals of 60 seconds
            if self.interval % 60 != 0:
                raise Exception('Interval must be a multiple of 60 seconds: %s'
                                % self.interval)
        if (('access_key_id' in self.config and
             'secret_access_key' in self.config)):
            self.auth_kwargs = {
                'aws_access_key_id': self.config['access_key_id'],
                'aws_secret_access_key': self.config['secret_access_key']
            }
        else:
            # If creds not present, assume we're using IAM roles with
            # instance profiles. Boto will automatically take care of using
            # the creds from the instance metatdata.
            self.auth_kwargs = {}

    def check_boto(self):
        if not boto3:
            self.log.error("boto3 module not found!")
            return False
        return True

    def get_default_config(self):
        """
        Return the default collector settings.
        """
        config = super(RdsCollector, self).get_default_config()
        config.update({
            'path': 'rds',
            'regions': ['us-east-1'],
            'interval': 60,
            'period': 300,
            'format': '$region.$rds_id.$metric_name',
        })
        return config

    def get_default_config_help(self):
        config_help = super(RdsCollector, self).get_default_config_help()
        config_help.update({
            'access_key_id': 'aws access key',
            'secret_access_key': 'aws secret key',
            'rds_ids': 'enter rds instance identifiers seperated by comma'
        })
        return config_help

    def get_rds_identifiers(self, region, session):
        """
        Get all rds instance identifiers for a given region based on
        configuration
        :param region: region to get rds instance identifiers for
        :param session: boto3 session to use
        :return: list of rds instance identifiers to collect metrics for
        """
        region_dict = self.config.get('regions', {}).get(region, {})
        region_rds_client = session.client('rds')
        response = region_rds_client.describe_db_instances()
        if 'rds_ids' in region_dict:
            # Regular expressions RDS instances we want to collect metrics on
            matchers = \
                [re.compile(regex) for regex in region_dict.get('rds_ids', [])]
            full_rds_ids = \
                [instance['DBInstanceIdentifier'] for instance in response['DBInstances']]
            rds_ids = []
            for rds_id in full_rds_ids:
                if matchers and any([m.match(rds_id) for m in matchers]):
                    rds_ids.append(rds_id)
        else:
            rds_ids = \
                [instance['DBInstanceIdentifier'] for instance in response['DBInstances']]
        return rds_ids

    def process_stat(self, region, rds_id, metric, stat):
        """
        Process a stat returned by cloudwatch.
        :param region: current aws region
        :param rds_id: current rds instance identifier
        :param metric: current metric
        :param stat: statistic returned by cloudwatch
        """
        template_tokens = {
            'region': region,
            'rds_id': rds_id,
            'metric_name': metric.name,
        }
        ttl = float(self.config['interval']) * float(
            self.config['ttl_multiplier'])
        name_template = Template(self.config['format'])
        formatted_name = name_template.substitute(template_tokens)
        path = self.get_metric_path(formatted_name, None)
        metric_to_publish = Metric(path, stat[metric.aws_type],
                                   raw_value=stat[metric.aws_type],
                                   timestamp=time.mktime(utc_to_local(stat['Timestamp']).timetuple()),
                                   precision=metric.precision, host=self.get_hostname(),
                                   metric_type=metric.diamond_type, ttl=ttl)
        self.publish_metric(metric_to_publish)

    def process_metric(self, region, region_cw_client, rds_id, start_time, end_time, metric):
        """
        Process a given cloudwatch metric for a given rds instance. Note that
        this method only emits the most recent cloudwatch stat for the metric.
        :param region: current aws region
        :param region_cw_client: cloudwatch boto3 client for the region
        :param rds_id: current rds instance identifier
        :param start_time: cloudwatch query start time
        :param end_time: cloudwatch query end time
        :param metric: current metric
        """
        response = region_cw_client.get_metric_statistics(
            Namespace='AWS/RDS',
            MetricName=metric.name,
            Dimensions=[
                {
                    'Name': 'DBInstanceIdentifier',
                    'Value': rds_id
                }
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=self.config['interval'],
            Statistics=[metric.aws_type])
        stats = response['Datapoints']
        # create a fake stat if the current metric should default to zero when
        # a stat is not returned. Cloudwatch just skips the metric entirely
        # instead of wasting space to store/emit a zero.
        if len(stats) == 0 and metric.default_to_zero:
            stats.append({
                u'Timestamp': start_time,
                metric.aws_type: 0.0,
                u'Unit': u'Count'
            })
        for stat in stats[-1:]:
            self.process_stat(region, rds_id, metric, stat)

    def process_instance(self, region, region_cw_client, rds_id, start_time, end_time):
        """
        Process a given rds instance by collecting all metrics.
        :param region: current aws region
        :param region_cw_client: cloudwatch boto3 client for the region
        :param rds_id: current rds instance identifier
        :param start_time: cloudwatch query start time
        :param end_time: cloudwatch query end time
        """
        for metric in self.metrics:
            self.process_metric(region, region_cw_client, rds_id, start_time, end_time, metric)

    def process_region(self, region, start_time, end_time):
        """
        Process all rds instance identifiers in the given region.
        :param region: current aws region
        :param start_time: cloudwatch query start time
        :param end_time: cloudwatch query end time
        """
        session = Session(aws_access_key_id=self.config['access_key_id'],
                          aws_secret_access_key=self.config['secret_access_key'],
                          region_name=region)
        region_cw_client = session.client('cloudwatch')
        for rds_id in self.get_rds_identifiers(region, session):
            self.process_instance(region, region_cw_client, rds_id, start_time, end_time)

    def collect(self):
        """
        Collect all metrics.
        """
        if not self.check_boto():
            return
        now = datetime.datetime.utcnow()
        end_time = now.replace(second=0, microsecond=0)
        start_time = end_time - datetime.timedelta(seconds=self.period)
        threads = []
        for region in self.config['regions'].keys():
            region_thread = threading.Thread(target=self.process_region,
                                             args=(region, start_time, end_time))
            region_thread.start()
            threads.append(region_thread)
        for thread in threads:
            thread.join()
