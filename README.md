# Gitential V2

## Requirements

* Python 3.8
* poetry
* docker-compose
* A .secrets.env file with `GITENTIAL2_TEST_SECRET` and `GITENTIAL_LICENSE` environment variable set

## Development environment setup

To start postgres, redis and pgadmin and install the python dependecies run the following commands:

```
docker-compose up -d
poetry install
```

## Starting the public-api

```
poetry shell
python -m gitential2 public-api -h 0.0.0.0 --reload
```

We are using the port 7999 for the API by default. And 7998 will be the frontend in the dev environment.


## Starting a celery worker

```
poetry shell
celery  -A gitential2.celery.main worker --loglevel=DEBUG
```

Instead of using poetry shell you can always use the `poetry run [cmd]...`

## Running the linters

```
poetry run inv lint
```

## Running the unit tests

```
poetry shell
source .secrets.env
inv unit-test
```