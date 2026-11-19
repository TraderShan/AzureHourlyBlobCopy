using './adf-private-access.bicep'

// Copy beside the template as a .bicepparam file. Replace all sample names and IDs.
var subscriptionId = '00000000-0000-0000-0000-000000000000'
param factoryName = 'example-data-factory'
param integrationRuntimeName = 'ir-hourly-copy-private'
param functionAppId = '/subscriptions/${subscriptionId}/resourceGroups/example-workload/providers/Microsoft.Web/sites/example-hourly-copy'
param invocationVaultId = '/subscriptions/${subscriptionId}/resourceGroups/example-workload/providers/Microsoft.KeyVault/vaults/example-hourly-keyvault'
