# v0.4

+ Don't attempt to re-use shards already moved
+ Add `--one-way` to help where shard count is imbalanced (perhaps for a subset of indices)
+ Improve printing of node/shard state
+ Stop optimising shards when there are no longer improvements

# v0.3

+ Add `--index-name` wildcard filter

# v0.2

+ Make it possible to provide multiple `--min-node` and `--max-node`

# v0.1

+ First pass at ES rebalancing script
