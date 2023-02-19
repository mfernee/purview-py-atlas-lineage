###############################################################################
### Global Variables
###############################################################################

KeyVaultName   = 'kv-adpoc-uks-01'
PurviewName    = 'pur-adpoc-uks-01'
CollectionName = 'n6ut7w'   # /root/Anomaly Detection/PartyA1

SourceFileName       = 'Sample-TX.parquet'
SourceFileFQDN       = 'https://stadpocdatauks02.dfs.core.windows.net/received-from-partyA1/adpoc/Transactions/Sample-TX.parquet'

InMemoryFileName     = 'Sample-TX-0001.in_memory'
InMemoryFileFQDN     = 'InMemory://Sample-TX-0001.in_memory'

TargetFileName       = 'model_output_partya1_01.csv'
TargetFileFQDN       = 'https://stadpocdatauks02.dfs.core.windows.net/model-output/PartyA1/model_output_PartyA1.csv'

ProcSource2MemTypeName       = 'Process'
ProcSource2MemName           = 'Decrypt columns'
ProcSource2MemQualifiedName  = 'process://Compute/DecryptColumns'

ProcMem2TargetTypeName       = 'Process'
ProcMem2TargetName           = 'Apply ML model'
ProcMem2TargetQualifiedName  = 'process://Compute/ApplyMLModel'

###############################################################################

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core.client import PurviewClient, PurviewCollectionsClient


class spnCredential:
    spnTenantId     = 'spn-tenant-id'
    spnClientId     = 'spn-client-id'
    spnClientSecret = 'spn-client-secret'

def connect_to_purview(keyVaultName, purviewName):
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

    client = PurviewClient(
        account_name=purviewName,
        authentication=oauth
    )   

    return client

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
purClient = connect_to_purview(keyVaultName=KeyVaultName, purviewName=PurviewName)
purColClient = connect_to_purviewCollection(keyVaultName=KeyVaultName, purviewName=PurviewName)

from pyapacheatlas.core.typedef import AtlasAttributeDef, EntityTypeDef
from pyapacheatlas.core.client  import AtlasEntity

###########################################################################################
### Create in-memory type definition
###########################################################################################
ent_def = EntityTypeDef(
  name = "in_memory",
  superTypes = ["DataSet"],
  attributes = [
    AtlasAttributeDef(
    name="someAttribute", typeName="string", 
    isOptional=True),
    AtlasAttributeDef(
    name="someIntList", typeName="array<int>", 
    isOptional=True, cardinality="SET",
    valuesMaxCount = 5)
  ]
)

results = purClient.upload_typedefs(entityDefs = [ent_def], force_update=True)

###########################################################################################

# Create data assets
aeSourceFileName = AtlasEntity(
    typeName = 'azure_datalake_gen2_path', 
    name = SourceFileName, 
    qualified_name= SourceFileFQDN)

aeTargetFileName = AtlasEntity(
    typeName = 'azure_datalake_gen2_path', 
    name = TargetFileName, 
    qualified_name= TargetFileFQDN)

aeInMemorAsset = AtlasEntity(
    typeName = 'in_memory', 
    name = InMemoryFileName, 
    qualified_name= InMemoryFileFQDN)

print('Uploading data assets')
results = purColClient.upload_entities([aeSourceFileName.to_json(),aeTargetFileName.to_json(),aeInMemorAsset.to_json()], 
    collection=CollectionName)
print(results)

# Create lineage
linSourceToMem = AtlasEntity(typeName = ProcSource2MemTypeName, name=ProcSource2MemName, qualified_name=ProcSource2MemQualifiedName)
attributes = {'name': ProcSource2MemName, 'qualifiedName': ProcSource2MemQualifiedName, 
    'inputs': [{ 'typeName': 'azure_datalake_gen2_path', 'uniqueAttributes': {'qualifiedName': SourceFileFQDN}}], 
    'outputs': [{ 'typeName': 'in_memory', 'uniqueAttributes': {'qualifiedName': InMemoryFileFQDN}}]
    }
linSourceToMem.attributes = attributes

linMemToTarget = AtlasEntity(typeName = ProcMem2TargetTypeName, name=ProcMem2TargetName, qualified_name=ProcMem2TargetQualifiedName)
attributes = {'name': ProcMem2TargetName, 'qualifiedName': ProcMem2TargetQualifiedName, 
    'inputs': [{ 'typeName': 'in_memory', 'uniqueAttributes': {'qualifiedName': InMemoryFileFQDN}}], 
    'outputs': [{ 'typeName': 'azure_datalake_gen2_path', 'uniqueAttributes': {'qualifiedName': TargetFileFQDN}}]
    }
linMemToTarget.attributes = attributes

print('Uploading lineage')
results = purColClient.upload_entities([linSourceToMem.to_json(),linMemToTarget.to_json()],
    collection=CollectionName)
print(results)
