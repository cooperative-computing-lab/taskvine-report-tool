#!/usr/bin/env python3
"""
Setup script for TaskVine Report Tool
"""

from setuptools import setup, find_packages
import os

# Read version from the package
def get_version():
    """Get version from package __init__.py"""
    version_file = os.path.join(os.path.dirname(__file__), 'taskvine_report', '__init__.py')
    with open(version_file, 'r') as f:
        for line in f:
            if line.startswith('__version__'):
                return line.split('=')[1].strip().strip('"').strip("'")
    return '0.1.0'

# Read long description from README
def get_long_description():
    """Get long description from README.md"""
    readme_file = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_file):
        with open(readme_file, 'r', encoding='utf-8') as f:
            return f.read()
    return ""

# Read requirements from requirements.txt
def get_requirements():
    """Get requirements from requirements.txt"""
    requirements_file = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    requirements = []
    if os.path.exists(requirements_file):
        with open(requirements_file, 'r') as f:
            requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    
    # Default requirements if file doesn't exist
    if not requirements:
        requirements = [
            'Flask>=2.0.0',
            'pandas>=1.3.0',
            'cloudpickle>=2.0.0',
            'tqdm>=4.60.0',
            'plotly>=5.0.0',
            'pytz>=2021.1',
            'graphviz>=0.17',
        ]
    
    return requirements

setup(
    # Package metadata
    name='taskvine-report',
    version=get_version(),
    description='Visualization and analysis tool for TaskVine execution logs',
    long_description=get_long_description(),
    long_description_content_type='text/markdown',
    
    # Author information
    author='Collaborative Computing Lab (CCL), University of Notre Dame',
    author_email='jzhou24@nd.edu',
    
    # Project URLs
    url='https://github.com/cooperative-computing-lab/taskvine-report-tool',
    project_urls={
        'Bug Reports': 'https://github.com/cooperative-computing-lab/taskvine-report-tool/issues',
        'Source': 'https://github.com/cooperative-computing-lab/taskvine-report-tool',
        'Documentation': 'https://ccl.cse.nd.edu/software/taskvine/',
        'CCL Homepage': 'https://ccl.cse.nd.edu/',
    },
    
    # Package structure
    packages=find_packages(),
    include_package_data=True,
    package_data={
        'taskvine_report': [
            'templates/*',
            'templates/**/*',
            'static/*',
            'static/**/*',
            'static/**/**/*',
        ],
    },
    
    # Dependencies
    install_requires=get_requirements(),
    
    # Python version requirement
    python_requires='>=3.7',
    
    # Classification
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Monitoring',
    ],
    
    # Keywords
    keywords='taskvine, workflow, visualization, monitoring, distributed-computing, ccl, notre-dame',
    
    # Console scripts - these create the vine_parse and vine_report commands
    entry_points={
        'console_scripts': [
            'vine_parse=taskvine_report.cli.parse:main',
            'vine_report=taskvine_report.cli.report:main',
        ],
    },
    
    # Development dependencies
    extras_require={
        'dev': [
            'pytest>=6.0',
            'pytest-cov>=2.0',
            'black>=21.0',
            'flake8>=3.8',
            'mypy>=0.800',
        ],
        'test': [
            'pytest>=6.0',
            'pytest-cov>=2.0',
        ],
    },
    
    # Zip safe
    zip_safe=False,
) 