#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# This is a terrible ugly hack of a script.

import sys
import logging
import argparse
import random
import string

from kubernetes import client, config
from kubernetes.client import Configuration

namespace = "load-test"
configmaps = dict()


def main(rounds, *args, **kwargs):
    global v1

    try:
        config.load_kube_config()
        version = client.VersionApi().get_code()
        logging.info(f"Connected to {Configuration._default.host} - {version.git_version}")
    except Exception as e:
        logging.error(f"Kubernetes version check failed: {e}")
        sys.exit(1)

    v1 = client.CoreV1Api()
    ns = client.V1Namespace(
            metadata=client.V1ObjectMeta(
                name=namespace))

    try:
        v1.create_namespace(ns)
    except client.exceptions.ApiException as e:
        if e.status == 409:
            pass
        else:
            raise e

    for _ in range(rounds):
        i = random.randint(0, 9)
        if i in [0, 1]:
            create_configmap()
        elif i in [2, 3, 4, 5, 6]:
            update_configmap()
        elif i in [7, 8]:
            delete_configmap()
        else:
            list_configmaps()


def update_or_merge_configmap(cm):
    while True:
        try:
            logging.debug(f"Updating {cm.metadata.name} rev={cm.metadata.resource_version}")
            return v1.replace_namespaced_config_map(name=cm.metadata.name, namespace=namespace, body=cm)
        except client.exceptions.ApiException as e:
            logging.debug(f"\tError: {e.status}")
            if e.status == 409 and 'StorageError: invalid object' in e.body:
                logging.debug("\tCreating and merging...")
                cm1 = create_or_get_configmap(cm)
                if not cm1.data:
                    cm1.data = dict()
                cm1.data.update(cm.data)
                cm = cm1
            elif e.status == 409:
                logging.debug("\tReading and merging...")
                try:
                    cm1 = v1.read_namespaced_config_map(name=cm.metadata.name, namespace=namespace)
                    if not cm1.data:
                        cm1.data = dict()
                    cm1.data.update(cm.data)
                    cm = cm1
                except client.exceptions.ApiException as e1:
                    if e1.status == 404:
                        pass
                    else:
                        raise e1
            else:
                raise e


def create_or_get_configmap(cm):
    cm.metadata.resource_version = ''
    while True:
        try:
            logging.debug(f"Creating {cm.metadata.name}")
            return v1.create_namespaced_config_map(namespace=namespace, body=cm)
        except client.exceptions.ApiException as e:
            logging.debug(f"\tError: {e.status}")
            if e.status == 409:
                logging.debug("\tReading existing...")
                try:
                    return v1.read_namespaced_config_map(name=cm.metadata.name, namespace=namespace)
                except client.exceptions.ApiException as e1:
                    if e1.status == 404:
                        pass
                    else:
                        raise e1
            else:
                raise e


def try_delete_configmap(cm):
    try:
        logging.debug(f"Deleting {cm.metadata.name} rev={cm.metadata.resource_version}")
        options = client.V1DeleteOptions(
                    preconditions=client.V1Preconditions(
                        resource_version=cm.metadata.resource_version))
        v1.delete_namespaced_config_map(name=cm.metadata.name, namespace=namespace, body=options)
        return True
    except client.exceptions.ApiException as e:
        logging.debug(f"\tError: {e.status}")
        if e.status == 404:
            return True
        elif e.status == 409:
            return False
        else:
            raise e


def create_configmap():
    i = random.randint(0, 99)
    cm = client.V1ConfigMap(
            metadata=client.V1ObjectMeta(
                name=f"test-{i:02}"))
    configmaps[i] = create_or_get_configmap(cm)


def update_configmap():
    if not configmaps:
        return
    i = random.choice(list(configmaps.keys()))
    j = random.randint(0, 99)
    k = random.randint(1, 16) * 256
    cm = configmaps[i]
    if not cm.data:
        cm.data = dict()
    cm.data[f"key-{j:02}"] = ''.join(random.choices(string.printable, k=k))
    configmaps[i] = update_or_merge_configmap(cm)


def delete_configmap():
    if not configmaps:
        return
    i = random.choice(list(configmaps.keys()))
    cm = configmaps[i]
    if try_delete_configmap(cm):
        del configmaps[i]


def list_configmaps():
    if not configmaps:
        return
    cml = v1.list_namespaced_config_map(namespace=namespace)
    for cm in cml.items:
        configmaps[cm.metadata.name] = cm


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--rounds', '-r', type=int, required=False, default=5000)
    parser.add_argument('--log-level', '-l', type=str, required=False, default='INFO')
    args = parser.parse_args()

    try:
        logging.basicConfig(level=args.log_level, format='[%(process)d]\t%(levelname).1s %(message)s')
        main(**vars(args))
    except KeyboardInterrupt:
        pass
    except Exception:
        logging.exception('Unhandled exception')
