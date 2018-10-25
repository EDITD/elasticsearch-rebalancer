import click
import requests

from humanize import naturalsize

from .util import (
    check_cluster_health,
    combine_nodes_and_shards,
    execute_reroute_commands,
    get_node_fs_stats,
    get_ordred_nodes_and_average_used,
    get_shards,
    get_transient_cluster_setting,
    set_transient_cluster_setting,
    wait_for_no_relocations,
)


class BalanceException(click.ClickException):
    def __init__(self, message):
        message = click.style(message, 'red')
        super(BalanceException, self).__init__(message)


def attempt_to_find_swap(nodes, shards):
    ordered_nodes, average = get_ordred_nodes_and_average_used(nodes)
    node_name_to_shards, index_to_node_names = combine_nodes_and_shards(nodes, shards)

    for node in reversed(ordered_nodes):  # biggest to smallest node
        if node['name'] in node_name_to_shards:
            max_node = node
            break
    else:
        raise BalanceException('Could not find max node with shards!')

    for node in ordered_nodes:
        if node['name'] in node_name_to_shards:
            min_node = node
            break
    else:
        raise BalanceException('Could not find min node with shards!')

    min_used = min_node['used_percent']
    max_used = max_node['used_percent']
    spread_used = round(max_used - min_used, 2)

    click.echo((
        f'> Disk used over {len(nodes)} nodes: '
        f'average={average}%, min={min_used}%, max={max_used}%, spread={spread_used}%'
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

    old_min_percent = min_node['used_percent']
    old_max_percent = max_node['used_percent']

    # Update the shard + node info for the next iteration
    max_shard['node'] = min_node['name']
    min_node['used_bytes'] += max_shard['size_in_bytes'] - min_shard['size_in_bytes']
    min_node['used_percent'] = round(
        min_node['used_bytes'] / min_node['total_bytes'] * 100, 2,
    )

    min_shard['node'] = max_node['name']
    max_node['used_bytes'] += min_shard['size_in_bytes'] - max_shard['size_in_bytes']
    max_node['used_percent'] = round(
        max_node['used_bytes'] / max_node['total_bytes'] * 100, 2,
    )

    click.echo((
        '> Recommended swap for: '
        f'{max_shard["id"]} ({naturalsize(max_shard["size_in_bytes"])}) <> '
        f'{min_shard["id"]} ({naturalsize(min_shard["size_in_bytes"])})'
    ))
    click.echo((
        f'  maxNode: {max_node["name"]} '
        f'({old_max_percent}% -> {max_node["used_percent"]}%)'
    ))
    click.echo((
        f'  minNode: {min_node["name"]} '
        f'({old_min_percent}% -> {min_node["used_percent"]}%)'
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

    for i, command in enumerate(commands, 1):
        print_command(command)
        execute_reroute_commands(es_host, [command])

        click.echo(
            f'Waiting for relocation to complete ({i}/{len(commands)})...',
        )
        wait_for_no_relocations(es_host)
        check_raise_health(es_host)  # check the cluster is still good


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
    help='Whether to actually execute the shard reroutes.',
)
def rebalance_elasticsearch(es_host, iterations=1, attr=None, commit=False):
    # Parse out any attrs
    attrs = {}
    if attr:
        for a in attr:
            try:
                key, value = a.split('=', 1)
            except ValueError:
                raise BalanceException('Invalid attr, specify as key=value!')
            attrs[key] = value

    click.echo()
    click.echo('# Elasticsearch Rebalancer')
    click.echo(f'> Target: {click.style(es_host, bold=True)}')
    click.echo()

    if commit:
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
        nodes = get_node_fs_stats(es_host, attrs=attrs)
        if not nodes:
            raise BalanceException(f'No nodes found!')

        click.echo(f'> Found {len(nodes)} nodes')
        click.echo()

        click.echo('Loading shards...')
        shards = get_shards(es_host, attrs=attrs)
        if not shards:
            raise BalanceException(f'No shards found!')

        click.echo(f'> Found {len(shards)} shards')
        click.echo()

        click.echo(f'Investigating rebalance options...')

        all_reroute_commands = []

        for i in range(iterations):
            click.echo(f'> Iteration {i}')
            reroute_commands = attempt_to_find_swap(nodes, shards)

            if reroute_commands:
                all_reroute_commands.extend(reroute_commands)

            click.echo()

        if commit:
            print_execute_reroutes(es_host, reroute_commands)

    # Always restore the previous rebalance setting
    finally:
        if commit:
            click.echo(
                f'Restoring previous rebalance setting ({previous_rebalance})...',
            )
            set_transient_cluster_setting(
                es_host, 'cluster.routing.rebalance.enable', previous_rebalance,
            )

    click.echo('# Cluster rebalanced!')
    click.echo()


if __name__ == '__main__':
    rebalance_elasticsearch()
