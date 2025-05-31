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

## Build & Upload

```bash
rm -rf dist/ build/ *.egg-info/
python setup.py sdist bdist_wheel
twine upload dist/*
```