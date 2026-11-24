# TaskVine Report Tool - Build & Deploy Guide

## Environment Setup

```bash
python -m pip install --upgrade pip build twine
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
python -m build
python -m twine check dist/*

# Only after CI, wheel-install acceptance, and an explicit release decision:
python -m twine upload dist/*
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

This package supports Python 3.10 through 3.13. CI installs and tests each
supported interpreter. Python 3.9 and older are rejected by package metadata.

```bash
conda create -n taskvine-env python=3.11
conda activate taskvine-env
conda install -c jinzhou5042 taskvine-report-tool
```

## Required release gates

```bash
python -m unittest discover -s tests -q
npm ci
npm audit --audit-level=high
npx playwright install chromium
npm run test:e2e
python -m build
python -m twine check dist/*
```

Also install the built wheel into a clean virtual environment and run
`vine_parse --help`, `vine_report --help`, `vine_report gc --help`, and
`vine_export --help`. Publishing to PyPI or Conda remains a credentialed,
explicit release action.
