from typing import cast, Optional
from structlog import get_logger
import stripe
from stripe.error import SignatureVerificationError
from gitential2.core.subscription import set_as_professional, set_as_free
from gitential2.datatypes.users import UserInDB, UserUpdate

from .context import GitentialContext

PAYMENT_METHOD_TYPES = ["card"]

logger = get_logger(__name__)


stripe.set_app_info("gitential", version="0.0.1", url="https://teszt")
stripe.api_version = "2020-08-27"
stripe.api_key = "sk_test_rTy8rIts8T5ePsU8Mu9G0tyG00zZWBqBSJ"


def get_user_by_customer(g: GitentialContext, customer_id: str) -> Optional[UserInDB]:
    customer = stripe.Customer.retrieve(customer_id)
    return g.backend.users.get_by_email(customer.email)


def create_checkout_session(
    g: GitentialContext,
    price_id: str,
    number_of_developers: int,
    user: UserInDB,
):
    def create_stripe_customer(user: UserInDB):
        customer = stripe.Customer.create(
            email=user.email,
            metadata={"user_id": user.id, "number_of_developers_requested": number_of_developers},
        )
        user_copy = user.copy()
        user_copy.stripe_customer_id = customer.id
        user_new = g.backend.users.update(user.id, cast(UserUpdate, user_copy))
        return customer, user_new

    if user.stripe_customer_id is None:
        stripe_customer, user = create_stripe_customer(user)
    else:
        stripe_customer = stripe.Customer.retrieve(user.stripe_customer_id)  # checking
        if "deleted" in stripe_customer:
            logger.info(
                "missing customer but we have stripe customer id, fixing...", customer_id=user.stripe_customer_id
            )
            stripe_customer, user = create_stripe_customer(user)
    stripe.Customer.modify(stripe_customer.id, metadata={"number_of_developers": number_of_developers})
    domain_url = g.backend.settings.web.base_url
    try:
        checkout_session = stripe.checkout.Session.create(
            success_url=domain_url + "?payment=true&session_id={CHECKOUT_SESSION_ID}",
            cancel_url=domain_url + "?payment=false",
            payment_method_types=PAYMENT_METHOD_TYPES,
            mode="subscription",
            customer=user.stripe_customer_id,
            line_items=[
                {
                    "price": price_id,
                    "quantity": number_of_developers,
                }
            ],
            metadata={
                "user_id": user.id,
            },
        )
        return {"session_id": checkout_session["id"]}
    except Exception as e:  # pylint: disable=broad-except
        print("error")
        print(e)


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


def process_webhook(g: GitentialContext, input_data: bytes, signature: str):
    try:
        event = stripe.Webhook.construct_event(input_data, signature, g.settings.stripe.webhook_secret)
    except (ValueError, SignatureVerificationError):
        logger.info("Payload verification error")
        return None
    if event.type == "customer.subscription.created":
        logger.info("new subscription created")
    if event.type == "customer.subscription.updated":
        logger.info("new subscription modified")
        customer_id = event["data"]["object"]["customer"]
        customer = stripe.Customer.retrieve(customer_id)
        if event.data.object["status"] == "active":
            developers = int(customer["metadata"]["number_of_developers"])
            set_as_professional(g, int(customer["metadata"]["user_id"]), developers, event)
    elif event.type == "customer.subscription.deleted":
        logger.info("new subscription deleted")
        customer_id = event["data"]["object"]["customer"]
        customer = stripe.Customer.retrieve(customer_id)
        stripe.Customer.modify(customer.id, metadata={"number_of_developers": ""})
        set_as_free(g, customer["metadata"]["user_id"])
    elif event.type == "customer.deleted":
        logger.info("customer deleted")
        email = event["data"]["object"]["email"]
        user = g.backend.users.get_by_email(email)
        if user:
            user.stripe_customer_id = None
            g.backend.users.update(user.id, cast(UserUpdate, user))
    else:
        logger.info("not handled stripe event", event_id=event.type)
    return None


def get_customer_portal_session(g: GitentialContext, user: UserInDB) -> dict:
    domain_url = g.backend.settings.web.base_url
    session = stripe.billing_portal.Session.create(customer=user.stripe_customer_id, return_url=domain_url)
    return {"url": session.url}


def get_checkout_session(session_id: str):
    try:
        checkout_session = stripe.checkout.Session.retrieve(session_id)
    except stripe.error.InvalidRequestError:
        logger.error("error get checkout session", session_id=session_id)
        raise
    return checkout_session
