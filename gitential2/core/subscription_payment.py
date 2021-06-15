from typing import List
import stripe

from gitential2.settings import IntegrationSettings

from .context import GitentialContext


stripe.set_app_info("gitential", version="0.0.1", url="https://teszt")
stripe.api_version = "2020-08-27"
stripe.api_key = ""


def get_stripe_setup():
    return {}


def create_checkout_session(
    g: GitentialContext, price_id: str, quantity: int, payment_method_type: List[str] = ("card")
):
    domain_url = g.backend.settings.web.base_url
    try:
        checkout_session = stripe.checkout.Session.create(
            success_url=domain_url + "/success.html?session_id={CHECKOUT_SESSION_ID}",
            cancel_url=domain_url + "/canceled.html",
            payment_method_types=payment_method_type,
            mode="subscription",
            line_items=[{"price": price_id, "quantity": quantity}],
        )
        return {"session_id": checkout_session["id"]}
    except Exception as e:
        print("error")


def get_subscription_status(g: GitentialContext, subscription_id):
    subscription = stripe.Subscription.retrieve(subscription_id)
    return subscription


def change_subscription(subscription_id: str, developer_num, pay_occurrence):
    subscription = stripe.Subscription.retrieve(subscription_id)

    stripe.Subscription.modify(
        subscription.id,
        cancel_at_period_end=False,
        proration_behavior="create_prorations",
        items=[
            {
                "id": subscription["items"]["data"][0].id,
                "price": "price_CBb6IXqvTLXp3f",
            }
        ],
    )


def delete_subscription(subs_id) -> dict:
    stripe.Subscription.delete(subs_id)
    return {'status': 'success'}


def process_webhook(input_data):
    pass


