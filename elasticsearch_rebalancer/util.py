from collections import defaultdict

import requests


def es_request(es_host, endpoint, method=requests.get, **kwargs):
    response = method(
        f'http://{es_host}/{endpoint}',
        **kwargs,
    )
    response.raise_for_status()
    return response.json()


def matches_attrs(attrs, match_attrs):
    match_attrs = match_attrs or {}
    attrs = attrs or {}
    return attrs == match_attrs


def check_es_cluster_health(es_host):
    health = es_request(es_host, '_cluster/health')
    if health['status'] != 'green':
        raise Exception('ES is not green!')

    relocating_shards = health['relocating_shards']
    if relocating_shards > 0:
        raise Exception(f'ES is already relocating {relocating_shards} shards!')


def get_node_fs_stats(es_host, attrs=None):
    nodes = es_request(es_host, f'_nodes/stats/fs')['nodes']
    filtered_nodes = []

    for node_id, node_data in nodes.items():
        if not matches_attrs(node_data.get('attributes'), attrs):
            continue

        node_data['id'] = node_id
        stats = node_data['fs']['total']
        node_data['used_bytes'] = stats['total_in_bytes'] - stats['available_in_bytes']
        node_data['total_bytes'] = stats['total_in_bytes']

        node_data['used_percent'] = round(
            node_data['used_bytes'] / node_data['total_bytes'] * 100, 2,
        )

        filtered_nodes.append(node_data)
    return filtered_nodes


def get_shards(es_host, attrs=None):
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
        if shard['index'] not in filtered_index_names:
            continue

        shard['id'] = f'{shard["index"]}-{shard["shard"]}'
        shard['size_in_bytes'] = int(shard['store'])

        filtered_shards.append(shard)

    return filtered_shards


def get_ordred_notes_and_average_used(nodes):
    sum_total_bytes = 0
    sum_used_bytes = 0

    for node in nodes:
        sum_total_bytes += node['total_bytes']
        sum_used_bytes += node['used_bytes']

    average_used_percentage = round(sum_used_bytes / sum_total_bytes * 100, 2)

    ordered_nodes = sorted(nodes, key=lambda node: node['used_percent'])
    return ordered_nodes, average_used_percentage


def combine_nodes_and_shards(nodes, shards):
    node_name_to_shards = defaultdict(list)
    index_to_node_names = defaultdict(list)

    for shard in shards:
        node_name_to_shards[shard['node']].append(shard)
        index_to_node_names[shard['index']].append(shard['node'])

    node_name_to_shards = {
        key: sorted(shards, key=lambda shard: shard['size_in_bytes'])
        for key, shards in node_name_to_shards.items()
    }

    return node_name_to_shards, index_to_node_names
