import os
from enum import Enum
from typing import Union

from redis.auth.idp import IdentityProviderInterface
from redis.auth.token_manager import RetryPolicy, TokenManagerConfig
from redis_entraid.cred_provider import (
    DEFAULT_DELAY_IN_MS,
    DEFAULT_EXPIRATION_REFRESH_RATIO,
    DEFAULT_LOWER_REFRESH_BOUND_MILLIS,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_TOKEN_REQUEST_EXECUTION_TIMEOUT_IN_MS,
    EntraIdCredentialsProvider,
)
from redis_entraid.identity_provider import (
    ManagedIdentityIdType,
    ManagedIdentityProviderConfig,
    ManagedIdentityType,
    ServicePrincipalIdentityProviderConfig,
    _create_provider_from_managed_identity,
    _create_provider_from_service_principal,
    DefaultAzureCredentialIdentityProviderConfig,
    _create_provider_from_default_azure_credential,
)
from tests.conftest import mock_identity_provider


class AuthType(Enum):
    MANAGED_IDENTITY = "managed_identity"
    SERVICE_PRINCIPAL = "service_principal"
    DEFAULT_AZURE_CREDENTIAL = "default_azure_credential"


def identity_provider(request) -> IdentityProviderInterface:
    if hasattr(request, "param"):
        kwargs = request.param.get("idp_kwargs", {})
    else:
        kwargs = {}

    if request.param.get("mock_idp", None) is not None:
        return mock_identity_provider()

    auth_type = kwargs.get("auth_type", AuthType.SERVICE_PRINCIPAL)
    config = get_identity_provider_config(request=request)

    if auth_type == AuthType.MANAGED_IDENTITY:
        return _create_provider_from_managed_identity(config)

    if auth_type == AuthType.DEFAULT_AZURE_CREDENTIAL:
        return _create_provider_from_default_azure_credential(config)

    return _create_provider_from_service_principal(config)


def get_identity_provider_config(
    request,
) -> Union[
    ManagedIdentityProviderConfig,
    ServicePrincipalIdentityProviderConfig,
    DefaultAzureCredentialIdentityProviderConfig,
]:
    if hasattr(request, "param"):
        kwargs = request.param.get("idp_kwargs", {})
    else:
        kwargs = {}

    auth_type = kwargs.pop("auth_type", AuthType.SERVICE_PRINCIPAL)

    if auth_type == AuthType.MANAGED_IDENTITY:
        return _get_managed_identity_provider_config(request)

    if auth_type == AuthType.DEFAULT_AZURE_CREDENTIAL:
        return _get_default_azure_credential_provider_config(request)

    return _get_service_principal_provider_config(request)


def _get_managed_identity_provider_config(request) -> ManagedIdentityProviderConfig:
    resource = os.getenv("AZURE_RESOURCE")
    id_value = os.getenv("AZURE_USER_ASSIGNED_MANAGED_ID", None)

    if hasattr(request, "param"):
        kwargs = request.param.get("idp_kwargs", {})
    else:
        kwargs = {}

    identity_type = kwargs.pop("identity_type", ManagedIdentityType.SYSTEM_ASSIGNED)
    id_type = kwargs.pop("id_type", ManagedIdentityIdType.OBJECT_ID)

    return ManagedIdentityProviderConfig(
        identity_type=identity_type,
        resource=resource,
        id_type=id_type,
        id_value=id_value,
        kwargs=kwargs,
    )


def _get_service_principal_provider_config(
    request,
) -> ServicePrincipalIdentityProviderConfig:
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_credential = os.getenv("AZURE_CLIENT_SECRET")
    tenant_id = os.getenv("AZURE_TENANT_ID")
    scopes = os.getenv("AZURE_REDIS_SCOPES", None)

    if hasattr(request, "param"):
        kwargs = request.param.get("idp_kwargs", {})
        token_kwargs = request.param.get("token_kwargs", {})
        timeout = request.param.get("timeout", None)
    else:
        kwargs = {}
        token_kwargs = {}
        timeout = None

    if isinstance(scopes, str):
        scopes = scopes.split(",")

    return ServicePrincipalIdentityProviderConfig(
        client_id=client_id,
        client_credential=client_credential,
        scopes=scopes,
        timeout=timeout,
        token_kwargs=token_kwargs,
        tenant_id=tenant_id,
        app_kwargs=kwargs,
    )


def _get_default_azure_credential_provider_config(
    request,
) -> DefaultAzureCredentialIdentityProviderConfig:
    scopes = os.getenv("AZURE_REDIS_SCOPES", ())

    if hasattr(request, "param"):
        kwargs = request.param.get("idp_kwargs", {})
        token_kwargs = request.param.get("token_kwargs", {})
    else:
        kwargs = {}
        token_kwargs = {}

    if isinstance(scopes, str):
        scopes = scopes.split(",")

    return DefaultAzureCredentialIdentityProviderConfig(
        scopes=scopes, app_kwargs=kwargs, token_kwargs=token_kwargs
    )


def get_entra_id_credentials_provider(request, cred_provider_kwargs):
    idp = identity_provider(request)
    expiration_refresh_ratio = cred_provider_kwargs.get(
        "expiration_refresh_ratio", DEFAULT_EXPIRATION_REFRESH_RATIO
    )
    lower_refresh_bound_millis = cred_provider_kwargs.get(
        "lower_refresh_bound_millis", DEFAULT_LOWER_REFRESH_BOUND_MILLIS
    )
    max_attempts = cred_provider_kwargs.get("max_attempts", DEFAULT_MAX_ATTEMPTS)
    delay_in_ms = cred_provider_kwargs.get("delay_in_ms", DEFAULT_DELAY_IN_MS)
    token_mgr_config = TokenManagerConfig(
        expiration_refresh_ratio=expiration_refresh_ratio,
        lower_refresh_bound_millis=lower_refresh_bound_millis,
        token_request_execution_timeout_in_ms=DEFAULT_TOKEN_REQUEST_EXECUTION_TIMEOUT_IN_MS,  # noqa
        retry_policy=RetryPolicy(
            max_attempts=max_attempts,
            delay_in_ms=delay_in_ms,
        ),
    )
    return EntraIdCredentialsProvider(
        identity_provider=idp,
        token_manager_config=token_mgr_config,
        initial_delay_in_ms=delay_in_ms,
    )
