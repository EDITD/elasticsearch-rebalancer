from setuptools import setup

REQUIREMENTS = (
    'click',
    'humanize',
    'requests',
)


if __name__ == '__main__':
    setup(
        name='elasticsearch-rebalancer',
        description='Internal tool for ES index/alias creation',
        version='0.3',
        author='EDITED devs',
        author_email='dev@edited.com',
        packages=[
            'elasticsearch_rebalancer',
        ],
        url='https://github.com/EDITD/elasticsearch-rebalancer',
        install_requires=REQUIREMENTS,
        entry_points={
            'console_scripts': (
                (
                    'es-rebalance='
                    'elasticsearch_rebalancer.__main__:rebalance_elasticsearch'
                ),
            ),
        },
    )
