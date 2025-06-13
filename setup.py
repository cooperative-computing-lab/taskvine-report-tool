#!/usr/bin/env python3
"""
Setup script for TaskVine Report Tool

Traditional setup.py configuration for reliable PyPI packaging.
"""

from setuptools import setup
import os
import re

def get_version():
    """Read version from __init__.py without importing the package"""
    init_path = os.path.join(os.path.dirname(__file__), 'taskvine_report', '__init__.py')
    with open(init_path, 'r', encoding='utf-8') as f:
        content = f.read()
    version_match = re.search(r'^__version__ = [\'"]([^\'"]*)[\'"]', content, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError('Unable to find version string.')

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="taskvine-report-tool",
    version=get_version(),
    author="Collaborative Computing Lab (CCL), University of Notre Dame",
    author_email="jzhou24@nd.edu",
    description="Visualization and analysis tool for TaskVine execution logs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cooperative-computing-lab/taskvine-report-tool",
    project_urls={
        "Documentation": "https://ccl.cse.nd.edu/software/taskvine/",
        "Repository": "https://github.com/cooperative-computing-lab/taskvine-report-tool",
        "Bug Reports": "https://github.com/cooperative-computing-lab/taskvine-report-tool/issues",
        "CCL Homepage": "https://ccl.cse.nd.edu/",
    },
    packages=["taskvine_report", "taskvine_report.cli", "taskvine_report.routes", "taskvine_report.src"],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering",
        "Topic :: System :: Distributed Computing",
        "Topic :: System :: Monitoring",
    ],
    python_requires=">=3.7,<3.12",
    install_requires=[
        "flask>=2.0.0",
        "pandas>=1.3.0",
        "cloudpickle>=2.0.0",
        "tqdm>=4.60.0",
        "bitarray>=2.0.0",
        "pytz>=2021.1",
        "graphviz>=0.17",
        "rich",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
            "black>=21.0",
            "flake8>=3.8",
            "mypy>=0.800",
        ],
        "test": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "vine_parse=taskvine_report.cli.parse:main",
            "vine_report=taskvine_report.cli.report:main",
        ],
    },
    include_package_data=True,
    package_data={
        "taskvine_report": [
            "templates/*",
            "templates/**/*", 
            "static/*",
            "static/**/*",
            "static/**/**/*",
        ],
    },
    keywords=["taskvine", "workflow", "visualization", "monitoring", "distributed-computing", "ccl", "notre-dame"],
) 