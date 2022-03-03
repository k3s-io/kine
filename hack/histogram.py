#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# This is a terrible ugly hack of a script.

import re
import sys
import logging
import argparse
import termplotlib as tpl

from codecs import decode
from kubernetes import client, config
from kubernetes.client import Configuration

METRIC = 'etcd_request_duration_seconds'


def main(type, *args, **kwargs):
    global v1

    try:
        config.load_kube_config()
        version = client.VersionApi().get_code()
        logging.info(f"Connected to {Configuration._default.host} - {version.git_version}")
    except Exception as e:
        logging.error(f"Kubernetes version check failed: {e}")
        sys.exit(1)

    res = client.ApiClient().call_api('/metrics', 'GET', _return_http_data_only=True, _preload_content=False)
    operations = {}
    prev_value = 0
    for line in res.readlines():
        match = re.search(r'(?P<metric>.+){(?P<labels>.+)} (?P<value>\d+)', decode(line))
        if match:
            labels = {}
            metric = match.group('metric')
            value = int(match.group('value'))

            if not metric.startswith(METRIC):
                continue

            for part in match.group('labels').split(','):
                k, v = part.split('=')
                labels[k] = v.strip('"')

            if not labels.get('type', '').endswith(type):
                continue

            if labels['operation'] not in operations:
                operations[labels['operation']] = {'counts': [], 'buckets': [], 'type': labels['type']}
                prev_value = 0

            if metric.endswith('_bucket'):
                operations[labels['operation']]['counts'].append(value - prev_value)
                operations[labels['operation']]['buckets'].append(labels['le'])
                prev_value = value
            elif metric.endswith('_sum'):
                operations[labels['operation']]['sum'] = value
            elif metric.endswith('_count'):
                operations[labels['operation']]['count'] = value

    for operation, stats in operations.items():
        print(f"\n{stats['sum'] / stats['count']:.3f}  average {kwargs['backend_name']} request duration (seconds): {operation} {stats['type']}")
        fig = tpl.figure()
        fig.barh(stats['counts'], stats['buckets'], max_width=50)
        fig.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--type', '-t', type=str, required=False, default='core.ConfigMap')
    parser.add_argument('--log-level', '-l', type=str, required=False, default='INFO')
    parser.add_argument('--backend-name', '-b', type=str, required=False, default='etcd')
    args = parser.parse_args()

    try:
        logging.basicConfig(level=args.log_level, format='[%(process)d]\t%(levelname).1s %(message)s')
        main(**vars(args))
    except KeyboardInterrupt:
        pass
    except Exception:
        logging.exception('Unhandled exception')
