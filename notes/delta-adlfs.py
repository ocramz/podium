# # delta-rs for ADLFS https://stackoverflow.com/a/78512894/2890063

from datetime import datetime
from azure.identity import DefaultAzureCredential
from deltalake import DeltaTable
import logging

DEFAULT_AZURE_STORAGE_SCOPE = "https://storage.azure.com/.default"
credential = DefaultAzureCredential()  # FIXME credential comes from elsewhere
azure_storage_token = credential.get_token(DEFAULT_AZURE_STORAGE_SCOPE)

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