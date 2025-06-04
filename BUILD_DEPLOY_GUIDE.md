# TaskVine Report Tool - Build & Deploy Guide

## Environment Setup

```bash
pip install --upgrade pip wheel twine
pip install "setuptools>=65.0,<68.0"
```

## PyPI Authentication

Create `~/.pypirc`:
```ini
[distutils]
index-servers = pypi

[pypi]
repository = https://upload.pypi.org/legacy/
username = __token__
password = pypi-your-full-token
```

```bash
chmod 600 ~/.pypirc
```

## PIP Upload

```bash
rm -rf dist/ build/ *.egg-info/
python setup.py sdist bdist_wheel
twine upload dist/*
```

## Conda Upload

```bash
# Create build environment
conda create -n conda-build-env conda-build anaconda-client -y python=3.11
conda activate conda-build-env

# Build conda package (from local source)

conda build conda-recipe/ --output-folder dist/

# Login and upload
anaconda login
anaconda upload dist/noarch/taskvine-report-tool-*.conda
```

## Python Version Requirement

This package is restricted to Python >=3.7,<3.12

```bash
conda create -n taskvine-env python=3.11
conda activate taskvine-env
conda install -c jinzhou5042 taskvine-report-tool
```