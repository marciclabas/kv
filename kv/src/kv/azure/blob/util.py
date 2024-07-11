from datetime import datetime, timedelta
from azure.storage.blob import BlobSasPermissions, generate_blob_sas, BlobClient

def blob_url(
  client: BlobClient,
  expiry: datetime | None = None,
  permission = BlobSasPermissions(read=True)
) -> str:
  account_name: str = client.account_name # type: ignore
  account_key = client.credential.account_key
  if expiry is None:
    expiry = datetime.now() + timedelta(days=int(1e6)) # aka never
  token = generate_blob_sas(account_name, client.container_name, client.blob_name, account_key=account_key, expiry=expiry, permission=permission)
  return f"{client.url}?{token}"