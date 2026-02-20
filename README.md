# Data Pipeline

Internal ETL and reporting pipeline for processing data across multiple business domains (sales, inventory, logistics, HR, finance, marketing, support, procurement, manufacturing, quality).

## Prerequisites

- [pyenv](https://github.com/pyenv/pyenv) for managing Python versions
- [Poetry](https://python-poetry.org/) 2.2.0

## Setup

1. Clone the repository:

```bash
git clone <repo-url>
cd swe-interview
```

2. Install the correct Python version with pyenv:

```bash
pyenv install 3.12.10
```

The repo includes a `.python-version` file, so pyenv will automatically use 3.12.10 in this directory.

3. Install Poetry:

```bash
curl -sSL https://install.python-poetry.org | python3 - --version 2.2.0
```

4. Install dependencies:

```bash
poetry install --no-root
```

## Project Structure

```
pipeline/
  config.py          # Environment and pipeline configuration
  deploy.py          # Production deployment logic
  domains/           # Business domain modules (sales, finance, hr, etc.)
  utils/             # Shared utilities, validators, and types
```
