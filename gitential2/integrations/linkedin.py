from gitential2.datatypes import UserInfoCreate
from .base import BaseIntegration, OAuthLoginMixin


class LinkedinIntegration(OAuthLoginMixin, BaseIntegration):
    def oauth_register(self):
        return {
            "api_base_url": "https://api.linkedin.com/v2/",
            "access_token_url": "https://www.linkedin.com/oauth/v2/accessToken",
            "authorize_url": "https://www.linkedin.com/oauth/v2/authorization",
            "client_kwargs": {
                "scope": "r_liteprofile r_emailaddress",
                "token_endpoint_auth_method": "client_secret_post",
            },
            "userinfo_endpoint": "me?projection=(id,firstName,lastName)",
            "client_id": self.settings.oauth.client_id,
            "client_secret": self.settings.oauth.client_secret,
        }

    def normalize_userinfo(self, data, token=None) -> UserInfoCreate:
        pass


example_result = {
    "token": {
        "access_token": "AQW4WOGxF16v6hO5JCLhr6DMmcpBaForbEJFoBKs-EebAqQ1OEF-edQakoQ3a49XDeou_XEfO2ij2qWD7BqM4fWCSxyw0Z3JSfN-C559UOSyStmK10MNNuVCvk3kXB3eacsfsXOUXXtjNpBzcPmyBgdDXq6rnWUTivfeFAAkpGY-h8yEPR_5SugJW8R3EPeGECio-_Zj6Bt5LEIgh-_VvRR-64313hxhbxn4o41-OMwhXlI9rAlvI0Icl0zxFWUWIoRE5vOF7YMpD7UaJeQixXCMB9iELsJeDGDBTiISTxoFV9_MT19kAqcYF6ec_29Qp5vzFXEYBnTewgdhi3nBAuoQxMUNnw",
        "expires_in": 5183999,
        "expires_at": 1616631850,
    },
    "user_info": {
        "firstName": {"localized": {"en_US": "László"}, "preferredLocale": {"country": "US", "language": "en"}},
        "lastName": {"localized": {"en_US": "Andrási"}, "preferredLocale": {"country": "US", "language": "en"}},
        "id": "MIkMuVnpcJ",
    },
}
