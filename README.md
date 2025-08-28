# ModelCat

## Introduction

This package provides public tools and utilities to be used alongside [ModelCat.ai](https://app.modelcat.ai/).

## Installation

ModelCat is currently supported on:
* Linux
* Windows 10/11
* macOS

### Prerequisites
* `AWS CLI v2`. For more information consult the installation guide: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
* `Python >=3.7` installation (you can use `conda` or virtual environments `venv`)
* `git` (if you intend to clone ModelCatConnector directly from github)

### Installing by cloning from GitHub

Start your terminal or command line.
```bash
# If using python venv or conda activate your environment e.g.:
conda activate modelcat_env
```

Clone and install the Python package:
```bash
cd ~/ # Will install in user folder
git clone git@github.com:Eta-Compute/modelcat.git
cd modelcat
pip install -e .
```

### Installing via `pip`

```bash
pip install modelcat
```

## User Guide

Currently the following functions are supported:
* ModelCatConnector for dataset validation and upload to ModelCat. 
See the [modelcat.connector README](src/modelcat/modelcatconnector/README.md) for details:
  * Tool setup (one-time only)
  * Dataset validation
  * Dataset upload


## Running tests

```
# install tox test runner:
python -m pip install --user tox
python -m tox --help

# install developer dependencies of the package
cd [...]/modelcat
pip install -e .[dev]

# run tests
tox
```

## Developed by:

<img src="img/logo.svg" alt="ModelCat Logo" width="200"/>