###############################################################################
### Global Variables
###############################################################################

KeyVaultName = 'kv-adpoc-uks-01'
PurviewName  = 'pur-adpoc-uks-01'

###############################################################################

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core.client import PurviewCollectionsClient


class spnCredential:
    spnTenantId     = 'spn-tenant-id'
    spnClientId     = 'spn-client-id'
    spnClientSecret = 'spn-client-secret'

def connect_to_purviewCollection(keyVaultName, purviewName):
    secretSPN = spnCredential()

    kvUri = f"https://{keyVaultName}.vault.azure.net"

    # connect to key vault
    credential = DefaultAzureCredential()
    kv = SecretClient(vault_url=kvUri, credential=credential)

    # Type will change the type to KeyVaultSecret
    secretSPN.spnTenantId = kv.get_secret(secretSPN.spnTenantId)
    secretSPN.spnClientId = kv.get_secret(secretSPN.spnClientId)
    secretSPN.spnClientSecret = kv.get_secret(secretSPN.spnClientSecret)

    # service principal autehntication
    oauth = ServicePrincipalAuthentication(
        tenant_id=secretSPN.spnTenantId.value,
        client_id=secretSPN.spnClientId.value,
        client_secret=secretSPN.spnClientSecret.value
    )

    client = PurviewCollectionsClient(f'https://{purviewName}.purview.azure.com/', authentication=oauth)   

    return client

# Connect to Purview
purColClient = connect_to_purviewCollection(keyVaultName=KeyVaultName, purviewName=PurviewName)

collections = purColClient.list_collections()
for value in collections:
    print(value)


print('Done.')