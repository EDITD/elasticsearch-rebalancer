from collections import deque
from time import sleep

import click
import requests


from .util import (
    check_cluster_health,
    combine_nodes_and_shards,
    execute_reroute_commands,
    format_shard_size,
    get_nodes,
    get_shard_size,
    get_shards,
    get_transient_cluster_setting,
    set_transient_cluster_setting,
    wait_for_no_relocations,
)


class BalanceException(click.ClickException):
    def __init__(self, message):
        message = click.style(message, 'red')
        super(BalanceException, self).__init__(message)


def find_node(nodes, node_name=None):
    if not isinstance(nodes, list):
        nodes = list(nodes)

    if not node_name:
        return nodes[0]

    for node in nodes:
        if node['name'] == node_name:
            return node

    raise ValueError(f'Could not find node: {node_name}')


def attempt_to_find_swap(
    nodes, shards,
    max_node_name=None,
    min_node_name=None,
    format_shard_weight_function=lambda weight: weight,
):
    ordered_nodes, node_name_to_shards, index_to_node_names = (
        combine_nodes_and_shards(nodes, shards)
    )

    min_node = find_node(ordered_nodes, min_node_name)
    max_node = find_node(reversed(ordered_nodes), max_node_name)

    average_weight = (
        sum(node['weight'] for node in ordered_nodes)
        / len(ordered_nodes)
    )

    min_weight = min_node['weight']
    max_weight = max_node['weight']
    spread_used = round(max_weight - min_weight, 2)

    click.echo((
        f'> Weight used over {len(nodes)} nodes: '
        f'average={average_weight}, min={min_weight}, max={max_weight}, spread={spread_used}'
    ))

    max_node_shards = node_name_to_shards[max_node['name']]
    min_node_shards = node_name_to_shards[min_node['name']]

    for shard in reversed(max_node_shards):  # biggest to smallest shard
        if min_node['name'] not in index_to_node_names[shard['index']]:
            max_shard = shard
            break
    else:
        raise BalanceException((
            'Could not find suitable large shard to move to '
            f'{min_node["name"]}!'
        ))

    for shard in min_node_shards:
        if max_node['name'] not in index_to_node_names[shard['index']]:
            min_shard = shard
            break
    else:
        raise BalanceException((
            'Could not find suitable small shard to move to '
            f'{max_node["name"]}!'
        ))

    # Update the shard + node info for the next iteration
    max_shard['node'] = min_node['name']
    min_node['weight'] += max_shard['weight'] - min_shard['weight']

    min_shard['node'] = max_node['name']
    max_node['weight'] += min_shard['weight'] - max_shard['weight']

    click.echo((
        '> Recommended swap for: '
        f'{max_shard["id"]} ({format_shard_weight_function(max_shard["weight"])}) <> '
        f'{min_shard["id"]} ({format_shard_weight_function(min_shard["weight"])})'
    ))
    click.echo((
        f'  maxNode: {max_node["name"]} '
        f'({format_shard_weight_function(max_weight)} '
        f'-> {format_shard_weight_function(max_node["weight"])})'
    ))
    click.echo((
        f'  minNode: {min_node["name"]} '
        f'({format_shard_weight_function(min_weight)} '
        f'-> {format_shard_weight_function(min_node["weight"])})'
    ))

    return [
        # Note: it's important that the large shard is moved off the full-er node
        # first in the case where we have to do them one by one.
        {
            'move': {
                'index': max_shard['index'],
                'shard': max_shard['shard'],
                'from_node': max_node['name'],
                'to_node': min_node['name'],
            },
        },
        {
            'move': {
                'index': min_shard['index'],
                'shard': min_shard['shard'],
                'from_node': min_node['name'],
                'to_node': max_node['name'],
            },
        },
    ]


def print_command(command):
    args = command['move']
    click.echo((
        f'> Executing reroute of {args["index"]}-{args["shard"]} '
        f'from {args["from_node"]} -> {args["to_node"]}'
    ))


def check_raise_health(es_host):
    # Check we're good to go
    try:
        check_cluster_health(es_host)
    except Exception as e:
        raise BalanceException(f'{e}')


def print_execute_reroutes(es_host, commands):
    try:
        execute_reroute_commands(es_host, commands)
    except requests.HTTPError as e:
        if e.response.status_code != 400:
            raise

    # Parallel reroute worked - so just wait & return
    else:
        for command in commands:
            print_command(command)

        click.echo('Waiting for relocations to complete...')
        wait_for_no_relocations(es_host)
        return

    # Now try to execute the reroutes one by one - it's likely that ES rejected the
    # parallel re-route because it would push the max node over the disk threshold.
    # So now attempt to reroute one shard at a time - first the big shard off the
    # big node, which should make space for the returning shard.
    if not click.confirm(click.style(
        'Parallel rerouting failed! Attempt shard by shard?',
        'yellow',
    )):
        raise BalanceException('User exited serial rerouting!')

    cluster_update_interval = get_transient_cluster_setting(
        es_host, 'cluster.info.update.interval', default='30s',
    )
    cluster_update_interval = int(cluster_update_interval[:-1])

    for i, command in enumerate(commands, 1):
        print_command(command)
        execute_reroute_commands(es_host, [command])

        click.echo(
            f'Waiting for relocation to complete ({i}/{len(commands)})...',
        )
        wait_for_no_relocations(es_host)
        check_raise_health(es_host)  # check the cluster is still good
        # Wait for minimum update interval or ES might still think there's not
        # enough space for the next reroute.
        sleep(cluster_update_interval + 1)


def make_rebalance_elasticsearch_cli(
    get_shard_weight_function=get_shard_size,
    format_shard_weight_function=format_shard_size,
):
    @click.command()
    @click.argument('es_host')
    @click.option(
        '--iterations',
        default=1,
        type=int,
        help='Number of iterations (swaps) to execute.',
    )
    @click.option(
        '--attr',
        multiple=True,
        help='Node attributes in form key=value.',
    )
    @click.option(
        '--commit',
        is_flag=True,
        default=False,
        help='Execute the shard reroutes (default print only).',
    )
    @click.option(
        '--print-state',
        is_flag=True,
        default=False,
        help='Print the current nodes & weights and exit.',
    )
    @click.option(
        '--max-node',
        default=None,
        multiple=True,
        help='Force the max node to consider for shard swaps.',
    )
    @click.option(
        '--min-node',
        default=None,
        multiple=True,
        help='Force the min node to consider for shard swaps.',
    )
    def rebalance_elasticsearch(
        es_host,
        iterations=1,
        attr=None,
        commit=False,
        print_state=False,
        max_node=None,
        min_node=None,
    ):
        # Parse out any attrs
        attrs = {}
        if attr:
            for a in attr:
                try:
                    key, value = a.split('=', 1)
                except ValueError:
                    raise BalanceException('Invalid attr, specify as key=value!')
                attrs[key] = value

        # Turn min/max node lists into deque instances
        if min_node:
            min_node = deque(min_node)
        if max_node:
            max_node = deque(max_node)

        click.echo()
        click.echo('# Elasticsearch Rebalancer')
        click.echo(f'> Target: {click.style(es_host, bold=True)}')
        click.echo()

        if commit:
            if print_state:
                raise click.ClickException('Cannot have --commit and --print-state!')

            # Check we have a healthy cluster
            check_raise_health(es_host)

            click.echo('Disabling cluster rebalance...')
            # Save the old value to restore later
            previous_rebalance = get_transient_cluster_setting(
                es_host, 'cluster.routing.rebalance.enable',
            )
            set_transient_cluster_setting(
                es_host, 'cluster.routing.rebalance.enable', 'none',
            )

        try:
            click.echo('Loading nodes...')
            nodes = get_nodes(es_host, attrs=attrs)
            if not nodes:
                raise BalanceException(f'No nodes found!')

            click.echo(f'> Found {len(nodes)} nodes')
            click.echo()

            click.echo('Loading shards...')
            shards = get_shards(
                es_host,
                attrs=attrs,
                get_shard_weight_function=get_shard_weight_function,
            )
            if not shards:
                raise BalanceException(f'No shards found!')

            click.echo(f'> Found {len(shards)} shards')
            click.echo()

            if print_state:
                click.echo('Nodes ordered by weight:')
                ordered_nodes, _, _ = combine_nodes_and_shards(nodes, shards)

                for node in ordered_nodes:
                    click.echo(f'> Node: {node["name"]}, weight: {node["weight"]}')

                return

            click.echo('Investigating rebalance options...')

            all_reroute_commands = []

            for i in range(iterations):
                click.echo(f'> Iteration {i}')
                reroute_commands = attempt_to_find_swap(
                    nodes, shards,
                    max_node_name=max_node[0] if max_node else None,
                    min_node_name=min_node[0] if min_node else None,
                    format_shard_weight_function=format_shard_weight_function,
                )

                if reroute_commands:
                    all_reroute_commands.extend(reroute_commands)

                click.echo()

                if min_node:
                    min_node.rotate()
                if max_node:
                    max_node.rotate()

            if commit:
                print_execute_reroutes(es_host, all_reroute_commands)

        except requests.HTTPError as e:
            click.echo(click.style(e.response.content, 'yellow'))
            raise BalanceException(f'Invalid ES response: {e.response.status_code}')

        # Always restore the previous rebalance setting
        finally:
            if commit:
                click.echo(
                    f'Restoring previous rebalance setting ({previous_rebalance})...',
                )
                set_transient_cluster_setting(
                    es_host, 'cluster.routing.rebalance.enable', previous_rebalance,
                )

        click.echo(f'# Cluster rebalanced with {len(all_reroute_commands)} reroutes!')
        click.echo()
    return rebalance_elasticsearch
