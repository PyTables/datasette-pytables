from setuptools import setup
import os

VERSION = '0.1'


def get_long_description():
    with open(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'README.md'
    ), encoding='utf8') as fp:
        return fp.read()


setup(
    name='datasette-pytables',
    description='Datasette connector for loading pytables files (.h5)',
    long_description=get_long_description(),
    long_description_content_type='text/markdown',
    author='Javier Sancho',
    url='https://github.com/jsancho-gpl/datasette-pytables',
    license='Apache License, Version 2.0',
    version=VERSION,
    packages=['datasette_pytables'],
    entry_points={
        'datasette.connectors': [
            'pytables = datasette_pytables'
        ],
    },
    install_requires=['datasette']
)

