import datetime
from typing import Optional
from enum import Enum
from .common import CoreModel, IDModelMixin, DateTimeModelMixin


class SubscriptionType(str, Enum):
    trial = "trial"
    professional = "professional"
    free = "free"


class StripeSubStatusType(str, Enum):
    active = "active"
    inactive = "inactive"


class CreateSession(CoreModel):
    number_of_developers: int


class SubscriptionBase(CoreModel):
    user_id: int
    subscription_start: datetime.datetime
    subscription_end: Optional[datetime.datetime]
    subscription_type: SubscriptionType = SubscriptionType.trial
    number_of_developers: int = 5
    stripe_subscription_id: Optional[str]
    stripe_subscription_status: StripeSubStatusType = StripeSubStatusType.inactive


class SubscriptionCreate(SubscriptionBase):
    @classmethod
    def default_for_new_user(cls, user_id):
        return cls(
            user_id=user_id,
            subscription_start=datetime.datetime.utcnow(),
            subscription_end=datetime.datetime.utcnow() + datetime.timedelta(days=14),
            subscription_type=SubscriptionType.trial,
            number_of_developers=5,
        )


class SubscriptionUpdate(SubscriptionBase):
    pass


class SubscriptionInDB(IDModelMixin, DateTimeModelMixin, SubscriptionBase):
    pass


class SubscriptionPublic(IDModelMixin, DateTimeModelMixin, SubscriptionBase):
    pass
