#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'pyodbc>=4.0.17',
    'sqlalchemy>=1.1.14'
    # TODO: put package requirements here
]

setup_requirements = [
    'pytest-runner',
    # TODO(uhjish): put setup requirements (distutils extensions, etc.) here
]

test_requirements = [
    'pytest',
    # TODO: put package test requirements here
]

setup(
    name='sqlalchemy_dremio',
    version='0.1.0',
    description="An SQLAlchemy dialect for Dremio which simply wraps the PyODBC Dremio interface.",
    long_description=readme + '\n\n' + history,
    author="Ajish George",
    author_email='yell@aji.sh',
    url='https://github.com/uhjish/sqlalchemy_dremio',
    packages=find_packages(include=['sqlalchemy_dremio']),
    entry_points={
        'sqlalchemy.dialects': [
            'dremio = sqlalchemy_dremio.pyodbc:DremioDialect_pyodbc',
            'dremio.pyodbc = sqlalchemy_dremio.pyodbc.DremioDialect_pyodbc',
        ]
    },
    include_package_data=True,
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='sqlalchemy_dremio',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
)
