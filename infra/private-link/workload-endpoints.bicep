// Adds private endpoints and DNS zone groups. Does not change shared services' public access or app configuration.
@minLength(1)
@description('Azure region of the VNet containing the endpoint subnet, not necessarily the destination storage region.')
param location string
@minLength(1)
@description('Resource ID of the customer VNet private endpoint subnet, separate from the Function integration subnet.')
param endpointSubnetId string
@minLength(1)
param sourceStorageId string
@minLength(1)
param destinationStorageId string
@minLength(1)
param hostStorageId string
@minLength(1)
@description('Resource ID of the Premium Azure Container Registry.')
param registryId string
@description('Map from short zone key to full private DNS zone resource ID. Use existing, reachable zones; see the deployment guide.')
param privateDnsZoneIds object
@description('Set only if the Function uses an Azure Files content share on the host account.')
param useHostFileShare bool = false
@description('Optional GPv2 account for private v3 reports and the recovery ledger.')
param recoveryStorageId string = ''
@description('Optional vault used by the Function worker for SAS or other app secrets. Not required for ADF-only vault access.')
param functionVaultId string = ''
@description('Optional Function App ID for a separate customer-VNet inbound endpoint, e.g. private operator/deployment access. ADF uses its own managed endpoint.')
param operatorFunctionAppId string = ''
@description('Optional Azure Monitor Private Link Scope ID. Leave blank if the network already reaches its approved AMPLS endpoint.')
param monitorScopeId string = ''
@description('Request manual approval on all new customer-VNet connections. Choose before first deployment.')
param requestManualApproval bool = false

// Normalize the IDs and deduplicate by resource/subresource so accounts shared between roles get one endpoint.
var storageTargets = union([
  { targetId: toLower(sourceStorageId), groupId: 'blob' }
  { targetId: toLower(destinationStorageId), groupId: 'blob' }
  { targetId: toLower(hostStorageId), groupId: 'blob' }
  { targetId: toLower(hostStorageId), groupId: 'queue' }
  { targetId: toLower(hostStorageId), groupId: 'table' }
], useHostFileShare ? [
  { targetId: toLower(hostStorageId), groupId: 'file' }
] : [], empty(recoveryStorageId) ? [] : [
  { targetId: toLower(recoveryStorageId), groupId: 'blob' }
  { targetId: toLower(recoveryStorageId), groupId: 'table' }
])

var storageEndpoints = [for target in storageTargets: {
  targetId: target.targetId
  groupId: target.groupId
  zoneKeys: [target.groupId]
}]
var targets = concat(storageEndpoints, [
  { targetId: toLower(registryId), groupId: 'registry', zoneKeys: ['acr'] }
], empty(functionVaultId) ? [] : [
  { targetId: toLower(functionVaultId), groupId: 'vault', zoneKeys: ['vault'] }
], empty(operatorFunctionAppId) ? [] : [
  { targetId: toLower(operatorFunctionAppId), groupId: 'sites', zoneKeys: ['sites'] }
], empty(monitorScopeId) ? [] : [
  { targetId: toLower(monitorScopeId), groupId: 'azuremonitor', zoneKeys: ['monitor', 'oms', 'ods', 'agentsvc', 'blob'] }
])

var connections = [for target in targets: {
  name: 'workload'
  properties: {
    groupIds: [target.groupId]
    privateLinkServiceId: target.targetId
  }
}]

resource endpoints 'Microsoft.Network/privateEndpoints@2024-05-01' = [for (target, i) in targets: {
  name: 'pe-${target.groupId}-${uniqueString(toLower(endpointSubnetId), target.targetId, target.groupId)}'
  location: location
  properties: {
    subnet: { id: endpointSubnetId }
    privateLinkServiceConnections: requestManualApproval ? [] : [connections[i]]
    manualPrivateLinkServiceConnections: requestManualApproval ? [connections[i]] : []
  }
}]

resource endpointDns 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-05-01' = [for (target, i) in targets: {
  parent: endpoints[i]
  name: 'default'
  properties: {
    privateDnsZoneConfigs: map(target.zoneKeys, key => {
      name: key
      properties: { privateDnsZoneId: privateDnsZoneIds[key] }
    })
  }
}]

output privateEndpointIds array = [for (target, i) in targets: endpoints[i].id]
