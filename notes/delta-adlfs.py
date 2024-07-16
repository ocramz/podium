# # delta-rs for ADLFS https://stackoverflow.com/a/78512894/2890063

from datetime import datetime
from azure.identity import DefaultAzureCredential
from deltalake import DeltaTable
import logging

DEFAULT_AZURE_STORAGE_SCOPE = "https://storage.azure.com/.default"
credential = DefaultAzureCredential()  # FIXME credential comes from elsewhere
azure_storage_token = credential.get_token(DEFAULT_AZURE_STORAGE_SCOPE)


# # token refresh decorator from https://stackoverflow.com/questions/71734360/python-rerun-code-with-new-token-when-token-has-expired 
# def authorize_on_expire(func):
#     def wrapper(token, *args, **kwargs):
#         try:
#             result = func(token, *args, **kwargs)
#         except exceptions.TokenExpiredException as e:
#             token = ... # token refreshing logic
#             result = func(token, *args, **kwargs)
#         finally:
#             return result
#     return wrapper

# # to be used as a @ decorator
def token_refresh(f):
  def wrapper(token, *args, **kwargs):
    try:
      y = f(token, *args, **kwargs)
    except Exception as e:  # TBD what exception is thrown when a stale token is used?
      token = credential.get_token(DEFAULT_AZURE_STORAGE_SCOPE)
      y = f(token, *args, **kwargs)
    finally:
      return y
  return wrapper

try:
  if datetime.fromtimestamp(azure_storage_token.expires_on) <= datetime.now():
    azure_storage_token = credential.get_token(DEFAULT_AZURE_STORAGE_SCOPE)
except Exception as e:
  logging.error(f"Access token retrieval failed: {e}")

data_path = "abfss://container@storageaccount.dfs.core.windows.net/delta-table"  # FIXME container, SA and path variables

storage_options = {
  "azure_storage_account_name": ACCOUNT_NAME,
  "azure_storage_token": azure_storage_token.token,
}

dt = DeltaTable(data_path, storage_options=storage_options)