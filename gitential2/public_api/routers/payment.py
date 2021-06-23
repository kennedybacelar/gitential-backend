from fastapi import APIRouter, Depends, Header, Request
from gitential2.core.context import GitentialContext
from gitential2.core.subscription_payment import (
    create_checkout_session,
    process_webhook,
    get_checkout_session,
)
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


@router.post("/workspaces/{workspace_id}/payment/customer-portal/")
def customer_portal(
    workspace_id: int,
    g: GitentialContext = Depends(gitential_context),
    current_user=Depends(current_user),
):
    check_permission(g, current_user, Entity.membership, Action.create, workspace_id=workspace_id)
    # return get_customer_portal_session(g, current_user)
    return {}


@router.get("/workspaces/{workspace_id}/payment/checkoutsession/")
def get_checkout(
    workspace_id: int,
    session_id: str,
    g: GitentialContext = Depends(gitential_context),
    current_user=Depends(current_user),
):
    check_permission(g, current_user, Entity.membership, Action.create, workspace_id=workspace_id)
    return get_checkout_session(session_id)


@router.post("/webhook/")
async def webhook_call(
    request: Request, g: GitentialContext = Depends(gitential_context), stripe_signature: str = Header(None)
):
    payload = await request.body()
    process_webhook(
        g,
        payload,
        stripe_signature,
    )
    return {}
