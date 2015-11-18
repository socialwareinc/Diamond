# coding=utf-8

"""
This EB collector collects metrics for one or more AWS Elastic Beanstalk environments

#### Configuration

Below is an example configuration for the EBCollector.
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
    # Optional - queries all environments if omitted, supports regex
    environment_names = spw-production
    environment_cnames = lal-production-vpc.*, sps-production-vpc.*, voices-production.*

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
except ImportError:
    boto3 = False

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

class BeanstalkCollector(diamond.collector.Collector):

    # default_to_zero means if cloudwatch does not return a stat for the
    # given metric, then just default it to zero.
    MetricInfo = namedtuple(
        'MetricInfo',
        'name aws_type diamond_type precision default_to_zero'
    )

    # AWS metrics for Elastic Beanstalk
    metrics = [
        MetricInfo('InstancesSevere', 'Average', 'GAUGE', 0, False),
        MetricInfo('InstancesDegraded', 'Average', 'GAUGE', 0, False),
        MetricInfo('InstancesWarning', 'Average', 'GAUGE', 0, False),
        MetricInfo('InstancesInfo', 'Average', 'GAUGE', 0, False),
        MetricInfo('InstancesOk', 'Average', 'GAUGE', 0, False),
        MetricInfo('InstancesPending', 'Average', 'GAUGE', 0, False),
        MetricInfo('InstancesUnknown', 'Average', 'GAUGE', 0, False),
        MetricInfo('InstancesNoData', 'Average', 'GAUGE', 0, False),
        MetricInfo('ApplicationRequestsTotal', 'Sum', 'GAUGE', 0, False),
        MetricInfo('ApplicationRequests5xx', 'Sum', 'GAUGE', 0, False),
        MetricInfo('ApplicationRequests4xx', 'Sum', 'GAUGE', 0, False),
        MetricInfo('ApplicationRequests3xx', 'Sum', 'GAUGE', 0, False),
        MetricInfo('ApplicationRequests2xx', 'Sum', 'GAUGE', 0, False),
        MetricInfo('ApplicationLatencyP10', 'Average', 'GAUGE', 4, False),
        MetricInfo('ApplicationLatencyP50', 'Average', 'GAUGE', 4, False),
        MetricInfo('ApplicationLatencyP75', 'Average', 'GAUGE', 4, False),
        MetricInfo('ApplicationLatencyP85', 'Average', 'GAUGE', 4, False),
        MetricInfo('ApplicationLatencyP90', 'Average', 'GAUGE', 4, False),
        MetricInfo('ApplicationLatencyP95', 'Average', 'GAUGE', 4, False),
        MetricInfo('ApplicationLatencyP99', 'Average', 'GAUGE', 4, False),
        MetricInfo('ApplicationLatencyP99.9', 'Average', 'GAUGE', 4, False)
    ]

    def process_config(self):
        super(BeanstalkCollector, self).process_config()
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
        config = super(BeanstalkCollector, self).get_default_config()
        config.update({
            'path': 'eb',
            'regions': ['us-east-1'],
            'interval': 60,
            'period': 300,
            'format': '$region.$environment_name.$metric_name',
        })
        return config

    def get_default_config_help(self):
        config_help = super(BeanstalkCollector, self).get_default_config_help()
        config_help.update({
            'access_key_id': 'aws access key',
            'secret_access_key': 'aws secret key',
            'environment_names': 'enter EB environment names seperated by comma'
        })
        return config_help

    def get_environment_names(self, region, session):
        """
        Get all EB environment names for a given region based on
        configuration
        :param region: region to get EB environment names for
        :param session: boto3 session to use
        :return: list of EB environment names to collect metrics for
        """
        region_dict = self.config.get('regions', {}).get(region, {})
        region_eb_client = session.client('elasticbeanstalk')
        response = region_eb_client.describe_environments()
        if any(item in ['environment_cnames', 'environment_names'] for item in region_dict):
            cname_matchers = \
                [re.compile(regex) for regex in region_dict.get('environment_cnames', [])]
            name_matchers = \
                [re.compile(regex) for regex in region_dict.get('environment_names', [])]
            environment_names = list()
            for environment in response['Environments']:
                if "CNAME" in environment:
                    if cname_matchers and any([m.match(environment['CNAME']) for m in cname_matchers]):
                        environment_names.append(environment['EnvironmentName'])
                if name_matchers and any([m.match(environment['EnvironmentName']) for m in name_matchers]):
                    environment_names.append(environment['EnvironmentName'])
        else:
            environment_names = \
                [environment['EnvironmentName'] for environment in response['Environments']]
        return set(environment_names)

    def process_stat(self, region, environment_name, metric, stat):
        """
        Process a stat returned by cloudwatch.
        :param region: current aws region
        :param environment_name: current EB environment name
        :param metric: current metric
        :param stat: statistic returned by cloudwatch
        """
        template_tokens = {
            'region': region,
            'environment_name': environment_name,
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

    def process_metric(self, region, region_cw_client, environment_name, start_time, end_time, metric):
        """
        Process a given cloudwatch metric for a given EB Environment. Note that
        this method only emits the most recent cloudwatch stat for the metric.
        :param region: current aws region
        :param region_cw_client: cloudwatch boto3 client for the region
        :param environment_name: current EB environment name
        :param start_time: cloudwatch query start time
        :param end_time: cloudwatch query end time
        :param metric: current metric
        """
        response = region_cw_client.get_metric_statistics(
            Namespace='AWS/ElasticBeanstalk',
            MetricName=metric.name,
            Dimensions=[
                {
                    'Name': 'EnvironmentName',
                    'Value': environment_name
                }
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=int(self.config['interval']),
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
        for stat in stats:
            self.process_stat(region, environment_name, metric, stat)

    def process_environment(self, region, region_cw_client, environment_name, start_time, end_time):
        """
        Process a given EB environment by collecting all metrics.
        :param region: current aws region
        :param region_cw_client: cloudwatch boto3 client for the region
        :param environment_name: current EB environment name
        :param start_time: cloudwatch query start time
        :param end_time: cloudwatch query end time
        """
        for metric in self.metrics:
            self.process_metric(region, region_cw_client, environment_name, start_time, end_time, metric)

    def process_region(self, region, start_time, end_time):
        """
        Process all EB environments in the given region.
        :param region: current aws region
        :param start_time: cloudwatch query start time
        :param end_time: cloudwatch query end time
        """
        session = boto3.session.Session(aws_access_key_id=self.config['access_key_id'],
                          aws_secret_access_key=self.config['secret_access_key'],
                          region_name=region)
        region_cw_client = session.client('cloudwatch')
        for environment_name in self.get_environment_names(region, session):
            self.process_environment(region, region_cw_client, environment_name, start_time, end_time)

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
