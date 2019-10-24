# Elasticsearch Rebalancer

A script that attempts to re-balance Elasticsearch shards by size without changing the existing balancing.

By default ES balances shards over nodes by considering:

+ The number of shards / node
+ The number of shards / index / node

Which is great if every shard is the same size, but in reality this is not the case. Without considering the size of shards (except for watermarks) it's possible to end up with some nodes almost at watermark alongside others that are almost empty. [This blog post](http://engineering.simplymeasured.com/dev-blog/2015/07/08/balancing-elasticsearch-cluster-by-shard-size.html) describes it well - this script is inspired by that project. As well as size this logic can be applied to any weighting of shards.

## How does it work?

The script is based around the idea of "swaps" - pairs of shards to relocate between the two nodes. Each iteration the script identifies the most-full and least-full nodes, searching through their largest/smallest shards to find a suitable swap. Ideally the `largest shard on the most-full node` and the `smallest shard on the least-full node` swap.

To maintain existing ES balances, shards are only considered if the node to move to does not have any other shard from the same index. This means the shards per node and shards per index per node remain the same, so ES shouldn't do any additional rebalancing.

## Usage

```
Usage: es-rebalance [OPTIONS] ES_HOST

Options:
  --iterations INTEGER  Number of iterations (swaps) to execute.
  --attr TEXT           Node attributes in form key=value.
  --commit              Execute the shard reroutes (default print only).
  --print-state         Print the current nodes & weights and exit.
  --index-name TEXT     Filter the indices for swaps by name, supports
                        wildcards.
  --max-node TEXT       Force the max node to consider for shard swaps.
  --min-node TEXT       Force the min node to consider for shard swaps.
  --one-way             Disables shard swaps and simply moves max -> min. Note
                        after ES rebalancing is restored ES will attempt to
                        rebalance itself according to it's own heuristics.
  --help                Show this message and exit.
```

## Custom Weighting

By default the `es-rebalance` script uses shard size (in bytes) as the weight indicator. It is possible to customise this by writing your own CLI - for example:

```py
from elasticsearch_rebalancer import make_rebalance_elasticsearch_cli

def get_shard_weight(shard):
    return 1

if __name__ == '__main__':
    rebalance_elasticsearch = make_rebalance_elasticsearch_cli(
        get_shard_weight_function=get_shard_weight,
    )
    rebalance_elasticsearch()
```
