
import stripe

stripe.set_app_info(
    'stripe-samples/checkout-single-subscription',
    version='0.0.1',
    url='https://github.com/stripe-samples/checkout-single-subscription')
stripe.api_version = '2020-08-27'
stripe.api_key = ""

def create_checkout_session():
    pass

def process_webhook():
    pass

def get_checkout_session():
    pass

