from gitential2.datatypes import UserInfoCreate
from .base import BaseIntegration, OAuthLoginMixin


def get_localized_value(name):
    key = "{}_{}".format(name["preferredLocale"]["language"], name["preferredLocale"]["country"])
    return name["localized"].get(key, "")


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
            "userinfo_endpoint": "me?projection=(id,firstName,lastName,profilePicture)",
            "client_id": self.settings.oauth.client_id,
            "client_secret": self.settings.oauth.client_secret,
        }

    def normalize_userinfo(self, data, token=None) -> UserInfoCreate:
        print(data)
        given_name = get_localized_value(data["firstName"])
        family_name = get_localized_value(data["lastName"])
        params = {
            "integration_name": self.name,
            "integration_type": "linkedin",
            "sub": str(data["id"]),
            "name": " ".join([given_name, family_name]),
            "preferred_username": " ".join([given_name, family_name]),
            "extra": data,
        }

        url = "emailAddress?q=members&projection=(elements*(handle~))"
        client = self.get_oauth2_client(token=token)
        api_base_url = self.oauth_register()["api_base_url"]
        resp = client.get(api_base_url + url)
        email_data = resp.json()

        elements = email_data.get("elements")
        if elements:
            handle = elements[0].get("handle~")
            if handle:
                email = handle.get("emailAddress")
                if email:
                    params["email"] = email

        return UserInfoCreate(**params)


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
