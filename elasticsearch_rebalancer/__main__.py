from . import make_rebalance_elasticsearch_cli


def rebalance_elasticsearch():
    make_rebalance_elasticsearch_cli()()


if __name__ == '__main__':
    rebalance_elasticsearch()
