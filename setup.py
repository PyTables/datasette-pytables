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
    url='https://github.com/jsancho-gpl/datasette-pytables',
    license='Apache License, Version 2.0',
    packages=['datasette_pytables'],
    entry_points={
        'datasette.connectors': [
            'pytables = datasette_pytables'
        ],
    },
    install_requires=[
        'datasette-core',
        'tables',
        'moz-sql-parser==1.3.18033',
        'mo-future==1.6.18072'
    ],
    tests_require=['pytest']
)
