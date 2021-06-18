from typing import Optional
from fastapi import APIRouter, Depends, Header, Body
from gitential2.core.context import GitentialContext
from gitential2.core.subscription_payment import create_checkout_session, process_webhook
from gitential2.datatypes.subscriptions import CreateSession
from gitential2.core.permissions import check_permission
from gitential2.datatypes.permissions import Entity, Action
from ..dependencies import gitential_context, current_user

router = APIRouter(tags=["payment"])


@router.post("/workspaces/{workspace_id}/payment/createsession/")
def create_checkout(
    createsession: CreateSession,
    workspace_id: int,
    g: GitentialContext = Depends(gitential_context),
    current_user=Depends(current_user),
):
    check_permission(g, current_user, Entity.membership, Action.create, workspace_id=workspace_id)
    return create_checkout_session(g, g.settings.stripe.price_id, createsession.number_of_developers, current_user)


@router.post("/webhook/")
def webhook_call(
    g: GitentialContext = Depends(gitential_context), payload=Body(...), stripe_signature: Optional[str] = Header(None)
):
    process_webhook(
        g,
        payload,
        stripe_signature,
    )
    return {}
