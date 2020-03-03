#!/usr/bin/env python

from distutils.core import setup

setup(
    name="bulk-api-python-client",
    version="0.0.1",
    description="",
    author="Mikela Clemmons & Donnell Muse",
    author_email="infra@pivotbio.com",
    url="https://github.com/pivotbio/bulk-api-python-client",
    packages=["bulk_api_client"],
    include_package_data=True,
    install_requires=[
        "pandas>=0.25.2",
        "requests~=2.22.0",
        "requests-cache~=0.5.0",
        "PyYAML>=5.1.2,<5.4.0",
    ],
    extras_require={
        "dev": [
            "pytest==5.3.5",
            "pytest-cov==2.8.1",
            "flake8==3.7.9",
            "pytest-randomly==3.2.1",
            "pytest-repeat==0.8.0",
            "pdoc3>=0.6.2,<0.8.0",
            "black~=19.10b0",
        ]
    },
)
