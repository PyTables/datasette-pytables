from setuptools import setup
import os

def get_version():
    with open('VERSION') as fd:
        return fd.read().strip()

def get_long_description():
    with open(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'README.md'
    ), encoding='utf8') as fp:
        return fp.read()


setup(
    name='datasette-pytables',
    version=get_version(),
    description='Datasette connector for loading pytables files (.h5)',
    long_description=get_long_description(),
    long_description_content_type='text/markdown',
    author='Javier Sancho',
    author_email='jsf@jsancho.org',
    url='https://github.com/jsancho-gpl/datasette-pytables',
    license='Apache License, Version 2.0',
    packages=['datasette_pytables'],
    entry_points={
        'datasette.connectors': [
            'pytables = datasette_pytables:PyTablesConnector'
        ],
    },
    install_requires=[
        'datasette-connectors>=2.0.0',
        'tables',
        'moz-sql-parser==3.32.20026',
        'mo-future==3.89.20246'
    ],
    tests_require=['pytest', 'aiohttp']
)
