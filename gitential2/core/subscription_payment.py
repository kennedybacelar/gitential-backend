from typing import List
from structlog import get_logger
import stripe  # pylint: disable=all
from stripe.error import SignatureVerificationError  # pylint: disable=all
from gitential2.datatypes.users import UserInDB

from .context import GitentialContext

logger = get_logger(__name__)


stripe.set_app_info("gitential", version="0.0.1", url="https://teszt")
stripe.api_version = "2020-08-27"
stripe.api_key = "sk_test_rTy8rIts8T5ePsU8Mu9G0tyG00zZWBqBSJ"


def create_checkout_session(
    g: GitentialContext,
    price_id: str,
    quantity: int,
    user: UserInDB,
    payment_method_type: List[str] = ("card",),
):
    domain_url = g.backend.settings.web.base_url
    try:
        checkout_session = stripe.checkout.Session.create(
            success_url=domain_url + "?payment=true&session_id={CHECKOUT_SESSION_ID}",
            cancel_url=domain_url + "?payment=false",
            payment_method_types=[payment_method_type],
            mode="subscription",
            line_items=[{"price": price_id, "quantity": quantity}],
            metadata={
                "user_id": user.id,
            },
        )
        return {"session_id": checkout_session["id"]}
    except Exception:  # pylint: disable=broad-except
        print("error")


def _get_stripe_subscription(subscription_id) -> dict:
    subscription = stripe.Subscription.retrieve(subscription_id)
    return subscription


def change_subscription(subscription_id: str, developer_num: int, pice_id: str):
    subscription = stripe.Subscription.retrieve(subscription_id)

    stripe.Subscription.modify(
        subscription.id,
        cancel_at_period_end=False,
        proration_behavior="create_prorations",
        items=[{"id": subscription["items"]["data"][0].id, "price": pice_id, "quantity": developer_num}],
    )


def delete_subscription(subs_id) -> dict:
    stripe.Subscription.delete(subs_id)
    return {"status": "success"}


def process_webhook(g: GitentialContext, input_data, signature):
    try:
        event = stripe.Webhook.construct_event(
            payload=input_data, sig_header=signature, secret=g.settings.stripe.webhook_secret
        )
        print(event)
    except (ValueError, SignatureVerificationError):
        return None
    return {}
