from collections import defaultdict
from fnmatch import fnmatch
from time import sleep

import requests

from humanize import naturalsize


def matches_attrs(attrs, match_attrs):
    for key, value in match_attrs.items():
        if attrs.get(key) != value:
            return False
    return True


def es_request(es_host, endpoint, method=requests.get, **kwargs):
    response = method(
        f'http://{es_host}/{endpoint}',
        **kwargs,
    )

    response.raise_for_status()
    return response.json()


def get_cluster_health(es_host):
    return es_request(es_host, '_cluster/health')


def get_cluster_settings(es_host):
    return es_request(es_host, '_cluster/settings')


def check_cluster_health(es_host):
    health = get_cluster_health(es_host)

    if health['status'] != 'green':
        raise Exception('ES is not green!')

    relocating_shards = health['relocating_shards']
    if relocating_shards > 0:
        raise Exception(f'ES is already relocating {relocating_shards} shards!')


def wait_for_no_relocations(es_host):
    while True:
        health = get_cluster_health(es_host)

        relocating_shards = health['relocating_shards']
        if not relocating_shards:
            break

        sleep(10)


def execute_reroute_commands(es_host, commands):
    es_request(es_host, '_cluster/reroute', method=requests.post, json={
        'commands': commands,
    })


def get_transient_cluster_setting(es_host, path, default=None):
    attrs = path.split('.')
    settings = get_cluster_settings(es_host)

    value = settings['transient']
    for attr in attrs:
        value = value.get(attr)
        if not value:
            return default
    return value


def set_transient_cluster_setting(es_host, path, value):
    es_request(es_host, '_cluster/settings', method=requests.put, json={
        'transient': {path: value},
    })


def get_nodes(es_host, attrs=None):
    nodes = es_request(es_host, f'_nodes/stats/fs')['nodes']
    filtered_nodes = []

    for node_id, node_data in nodes.items():
        if not matches_attrs(node_data.get('attributes'), attrs):
            continue

        node_data['id'] = node_id
        filtered_nodes.append(node_data)
    return filtered_nodes


def get_shard_size(shard):
    return int(shard['store'])


def format_shard_size(weight):
    return naturalsize(weight, binary=True)


def get_shards(
    es_host,
    attrs=None,
    index_name_filter=None,
    get_shard_weight_function=get_shard_size,
):
    indices = es_request(es_host, '_settings')

    filtered_index_names = []

    for index_name, index_data in indices.items():
        index_settings = index_data['settings']['index']
        index_attrs = (
            index_settings
            .get('routing', {})
            .get('allocation', {})
            .get('require', {})
        )
        if not matches_attrs(index_attrs, attrs):
            continue

        if index_name_filter and not fnmatch(index_name, index_name_filter):
            continue

        filtered_index_names.append(index_name)

    shards = es_request(
        es_host, '_cat/shards',
        params={
            'format': 'json',
            'bytes': 'b',
        },
    )

    filtered_shards = []

    for shard in shards:
        if (
            shard['state'] != 'STARTED'
            or shard['index'] not in filtered_index_names
        ):
            continue

        shard['id'] = f'{shard["index"]}-{shard["shard"]}'
        shard['weight'] = get_shard_weight_function(shard)

        filtered_shards.append(shard)
    return filtered_shards


def combine_nodes_and_shards(nodes, shards):
    node_name_to_shards = defaultdict(list)
    index_to_node_names = defaultdict(list)

    for shard in shards:
        node_name_to_shards[shard['node']].append(shard)
        index_to_node_names[shard['index']].append(shard['node'])

    node_name_to_shards = {
        key: sorted(shards, key=lambda shard: shard['weight'])
        for key, shards in node_name_to_shards.items()
    }

    ordered_nodes = []
    for node in nodes:
        if node['name'] not in node_name_to_shards:
            continue

        node['weight'] = sum(
            shard['weight'] for shard in node_name_to_shards[node['name']]
        )

        ordered_nodes.append(node)

    ordered_nodes = sorted(ordered_nodes, key=lambda node: node['weight'])

    # min_weight = ordered_nodes[0]['weight']
    max_weight = ordered_nodes[-1]['weight']

    for node in ordered_nodes:
        node['weight_percentage'] = round((node['weight'] / max_weight) * 100, 2)

    return ordered_nodes, node_name_to_shards, index_to_node_names
