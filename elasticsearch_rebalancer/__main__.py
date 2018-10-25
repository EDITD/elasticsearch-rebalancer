import click
import requests

from humanize import naturalsize

from .util import (
    check_es_cluster_health,
    combine_nodes_and_shards,
    es_request,
    get_node_fs_stats,
    get_ordred_notes_and_average_used,
    get_shards,
)


class BalanceException(click.ClickException):
    pass


def attempt_to_find_swap(nodes, shards):
    ordered_nodes, average = get_ordred_notes_and_average_used(nodes)
    node_name_to_shards, shard_id_to_node_names = combine_nodes_and_shards(nodes, shards)

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
        if min_node['name'] not in shard_id_to_node_names[shard['id']]:
            max_shard = shard
            break
    else:
        raise BalanceException((
            'Could not find suitable large shard to move to '
            f'{min_node["name"]}!'
        ))

    for shard in min_node_shards:
        if max_node['name'] not in shard_id_to_node_names[shard['id']]:
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


def execute_reroute(es_host, reroute_commands):
    es_request(es_host, '_cluster/reroute', method=requests.post, json={
        'commands': reroute_commands,
    })


@click.command()
@click.argument('es_host')
@click.option('--iterations', default=1, type=int)
@click.option('--attr', multiple=True)
@click.option('--commit', is_flag=True, default=False)
def rebalance_elasticsearch(es_host, iterations, attr, commit=False):
    click.echo()
    click.echo('# Elasticsearch Rebalancer')
    click.echo(f'> Target: {click.style(es_host, bold=True)}')
    click.echo()

    # Don't continue if we're not green and relocating nothing!
    try:
        check_es_cluster_health(es_host)
    except Exception as e:
        if commit:
            raise BalanceException(e)
        else:
            click.echo(
                f'{click.style("Warning", bold=True)}: '
                f'{click.style(f"{e}", "yellow")}'
                ', would not continue with --commit!\n'
            )

    # Parse out attrs
    attrs = {}
    for a in attr:
        try:
            key, value = a.split('=', 1)
        except ValueError:
            raise BalanceException('Invalid attr, specify as key=value!')
        attrs[key] = value

    click.echo('Loading nodes...')
    nodes = get_node_fs_stats(es_host, attrs=attrs)
    click.echo(f'> Found {len(nodes)} nodes')
    click.echo()

    click.echo('Loading shards...')
    shards = get_shards(es_host, attrs=attrs)
    click.echo(f'> Found {len(shards)} shards')
    click.echo()

    if not nodes:
        raise BalanceException(
            f'No nodes found!',
        )

    click.echo(f'Investigating rebalance options...')

    for i in range(iterations):
        click.echo(f'> Iteration {i}')
        reroute_commands = attempt_to_find_swap(nodes, shards)
        if reroute_commands and commit:
            execute_reroute(es_host, reroute_commands)

        click.echo()


if __name__ == '__main__':
    rebalance_elasticsearch()
