# Gitential V2

## Requirements

* Python 3.8
* poetry
* docker-compose
* A .secrets.env file with `GITENTIAL2_TEST_SECRET` and `GITENTIAL_LICENSE` environment variable set

## Development environment setup

To start postgres, redis and pgadmin and install the python dependencies run the following commands:

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

Instead of using poetry shell, you can always use the `poetry run [cmd]...`

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

## Stripe itegration setup

The stack implemented with stripe financial integration.
Necessary configs in settings.yml:

```
stripe:
  enabled: True 
  publishable_key: "pk_test_8NvkMx8TUGYMVgxgHda7Ly4400OqJK6Caz" # this key retrieve to frontend
  private_key: "sk_test_rTy8rIts8T5ePsU8Mu9G0tyG00zZWBqBSJ" # API key secret
  price_id: "price_1Ix7JAFFL0UUh0zz2fXVMjcj" # the price which sold by
  webhook_secret: "whsec_J3wwXzXQOklhlC5qS9iCFynVkiHzXZ9g" # webhook secret, MUST redefine for local testing
```

### webhook:
For development this is essential to use webhook. All changes from stripe provider comes from webhook API 
call asyncrhonously.  
For local testing, you have to install stripe CLI to receive webhook calls. If the target system is available on 
internet, you can define own webhook secret in stripe dashboard.  
Running stripe CLI to catch webhooks:
```
stripe listen --forward-to localhost:7999/v2/webhook/
```
You can create triggers in dashboard as well, but you can use CLI for this:
```
stripe trigger customer.subscription.created
```
You can get stripe CLI: https://stripe.com/docs/stripe-cli

### Local development:
For local development, you have to setup:
* access to development dashboard in stripe
* install CLI
* Run CLI for webhook forward
* running stack
About webhooks: https://stripe.com/docs/webhooks
  
### Basic workflow:
The stripe integration follows a simple rule:
* User clicks on subscribe
* backend creates a session (with customer ID) and creates stripe customer if exists
* frontend using session id forwards to stripe 
* if payment successful, stripe sends several webhooks, and process it,