import os
from datetime import timedelta
from requests_cache import CachedSession
from config import (
    EHR_AUTH_METHOD,EHR_SERVER_USER, EHR_SERVER_PASSWORD,
    FHIR_SERVER_PASSWORD, FHIR_AUTH_METHOD
)


def create_session(
    cache_name='default_cache',
    expire_days=1,
    auth_method='basic',
    username=None,
    password=None,
    token=None
):
    """
    Create and return a cached requests session.

    :param cache_name:   Name of the cache directory/file (default 'default_cache').
    :param expire_days:  Number of days after which cached responses expire.
    :param auth_method:  Authentication method ('basic', 'bearer', or 'none').
    :param username:     Username for basic auth.
    :param password:     Password for basic auth.
    :param token:        Bearer token if using token-based auth.
    :return:             A requests_cache.CachedSession object with appropriate auth & caching.
    """
    session = CachedSession(
        cache_name=cache_name,
        use_cache_dir=True,
        cache_control=True,
        expire_after=timedelta(days=expire_days),
        allowable_codes=[200, 400],
        allowable_methods=['GET', 'POST', 'PUT', 'DELETE'],
        ignored_parameters=['api_key'],
        match_headers=['Accept-Language'],
        stale_if_error=True,
    )

    # Normalize auth_method to lowercase for consistency
    auth_method = (auth_method or 'basic').lower()

    if auth_method == 'basic' and username and password:
        # Attach HTTP Basic Auth
        session.auth = (username, password)
    elif auth_method == 'bearer' and token:
        # Attach Bearer token as Authorization header
        session.headers.update({"Authorization": f"Bearer {token}"})
    else:
        # 'none' or unrecognized method → no auth
        pass

    return session


# Example: Creating sessions for openEHR and FHIR servers.
# You might do this in main.py or wherever you need them.

ehr_session = create_session(
    cache_name='ehr_cache',
    auth_method=EHR_AUTH_METHOD,
    username=EHR_SERVER_USER,
    password=EHR_SERVER_PASSWORD
)

fhir_session = create_session(
    cache_name='fhir_cache',
    auth_method=FHIR_AUTH_METHOD,   # 'basic', 'bearer', or 'none'
    token=FHIR_SERVER_PASSWORD      # If FHIR_AUTH_METHOD == 'bearer', set token here
)
