#!/usr/bin/env python

from distutils.core import setup

setup(
    name='bulk-api-python-client',
    version='0.0.1',
    description='',
    author='Mikela Clemmons & Donnell Muse',
    author_email='infra@pivotbio.com',
    url='https://github.com/pivotbio/bulk-api-python-client',
    install_requires=[
        'pandas~=0.24.2',
        'requests~=2.22.0',
    ],
    extras_require={
        'dev': [
            'pytest==4.0.0',
            'pytest-cov==2.7.1',
            'flake8==3.7.7',
            'pytest-randomly==3.0.0',
        ]
    }
)
