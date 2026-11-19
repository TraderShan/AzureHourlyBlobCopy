using './workload-endpoints.bicep'

// Copy beside the template as a .bicepparam file. Replace all sample names and IDs.
var subscriptionId = '00000000-0000-0000-0000-000000000000'
var storagePrefix = '/subscriptions/${subscriptionId}/resourceGroups/example-workload/providers/Microsoft.Storage/storageAccounts'
var dnsPrefix = '/subscriptions/${subscriptionId}/resourceGroups/example-dns/providers/Microsoft.Network/privateDnsZones'

param location = 'eastus'
param endpointSubnetId = '/subscriptions/${subscriptionId}/resourceGroups/example-network/providers/Microsoft.Network/virtualNetworks/example-copy-vnet/subnets/private-endpoints'
param sourceStorageId = '${storagePrefix}/examplesource'
param destinationStorageId = '${storagePrefix}/exampledestination'
param hostStorageId = '${storagePrefix}/examplefunctionhost'
param registryId = '/subscriptions/${subscriptionId}/resourceGroups/example-workload/providers/Microsoft.ContainerRegistry/registries/examplecontainerregistry'
param privateDnsZoneIds = {
  blob: '${dnsPrefix}/privatelink.blob.core.windows.net'
  queue: '${dnsPrefix}/privatelink.queue.core.windows.net'
  table: '${dnsPrefix}/privatelink.table.core.windows.net'
  acr: '${dnsPrefix}/privatelink.azurecr.io'
  // Add the corresponding entries from the guide before enabling optional features.
}
// Optional: useHostFileShare, recoveryStorageId, functionVaultId, operatorFunctionAppId,
// monitorScopeId, requestManualApproval. See the guide for prerequisites and DNS keys.
