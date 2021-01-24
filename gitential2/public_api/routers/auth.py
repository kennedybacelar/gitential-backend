from fastapi import APIRouter, Request, HTTPException


async def handle_authorize(remote, token, user_info, request):
    return {"token": token, "user_info": user_info}


router = APIRouter()


@router.get("/auth/{backend}")
async def auth(
    backend: str,
    id_token: str = None,
    code: str = None,
    oauth_verifier: str = None,
    request: Request = None,
):
    remote = request.app.state.oauth.create_client(backend)
    if remote is None:
        raise HTTPException(404)

    if code:
        token = await remote.authorize_access_token(request)
        if id_token:
            token["id_token"] = id_token
    elif id_token:
        token = {"id_token": id_token}
    elif oauth_verifier:
        # OAuth 1
        token = await remote.authorize_access_token(request)
    else:
        # handle failed
        return await handle_authorize(remote, None, None, None)
    if "id_token" in token:
        user_info = await remote.parse_id_token(request, token)
    else:
        remote.token = token
        user_info = await remote.userinfo(token=token)
    return await handle_authorize(remote, token, user_info, request)


@router.get("/login/{backend}")
async def login(backend: str, request: Request):
    remote = request.app.state.oauth.create_client(backend)
    if remote is None:
        raise HTTPException(404)

    redirect_uri = request.url_for("auth", backend=backend)
    # conf_key = "{}_AUTHORIZE_PARAMS".format(backend.upper())
    # params = request.app.state.oauth.config.get(conf_key, default={}) if request.app.state.oauth.config else {}
    # return await remote.authorize_redirect(request, redirect_uri, **params)
    return await remote.authorize_redirect(request, redirect_uri)