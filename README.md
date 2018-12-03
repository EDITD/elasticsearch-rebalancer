# Elasticsearch Rebalancer

A script that attempts to re-balance Elasticsearch shards by size without changing the existing balancing.

By default ES balances shards over nodes by considering:

+ The number of shards / node
+ The number of shards / index / node

Which is great if every shard is the same size, but in reality this is not the case. Without considering the size of shards (except for watermarks) it's possible to end up with some nodes almost at watermark alongside others that are almost empty. [This blog post](http://engineering.simplymeasured.com/dev-blog/2015/07/08/balancing-elasticsearch-cluster-by-shard-size.html) describes it well - this script is inspired by that project.

## How does it work?

The script is based around the idea of "swaps" - pairs of shards to relocate between the two nodes. Each iteration the script identifies the most-full and least-full nodes, searching through their largest/smallest shards to find a suitable swap. Ideally the `largest shard on the most-full node` and the `smallest shard on the least-full node` swap.

To maintain existing ES balances, shards are only considered if the node to move to does not have any other shard from the same index. This means the shards per node and shards per index per node remain the same, so ES shouldn't do any additional rebalancing.

## Usage

```
Usage: es-rebalance [OPTIONS] ES_HOST

Options:
  --iterations INTEGER  Number of iterations (swaps) to execute.
  --attr TEXT           Node attributes in form key=value.
  --commit              Whether to actually execute the shard reroutes.
  --help                Show this message and exit.
```
