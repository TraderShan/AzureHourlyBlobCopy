// Deploy in the existing Data Factory's resource group. No pipeline or linked service is published here.
@minLength(1)
param factoryName string
@minLength(3)
@maxLength(63)
param integrationRuntimeName string = 'ir-hourly-copy-private'
@description('Set false to keep an existing Azure IR already attached to this factory’s managed VNet.')
param deployIntegrationRuntime bool = true
@minLength(1)
@description('Resource ID of the Elastic Premium Function App (not a slot).')
param functionAppId string
@minLength(1)
@description('Resource ID of the vault that holds the ADF invocation key, not the transfer SAS vault unless they are shared.')
param invocationVaultId string
param functionEndpointName string = 'mpe-hourly-copy-function'
param vaultEndpointName string = 'mpe-hourly-copy-invocation-vault'

resource factory 'Microsoft.DataFactory/factories@2018-06-01' existing = {
  name: factoryName
}

resource managedNetwork 'Microsoft.DataFactory/factories/managedVirtualNetworks@2018-06-01' = {
  parent: factory
  name: 'default'
  properties: {}
}

resource integrationRuntime 'Microsoft.DataFactory/factories/integrationRuntimes@2018-06-01' = if (deployIntegrationRuntime) {
  parent: factory
  name: integrationRuntimeName
  properties: {
    type: 'Managed'
    managedVirtualNetwork: {
      referenceName: managedNetwork.name
      type: 'ManagedVirtualNetworkReference'
    }
    typeProperties: {
      computeProperties: { location: 'AutoResolve' }
    }
  }
  dependsOn: [managedNetwork] // The IR is a sibling and must wait for the managed VNet.
}

resource functionEndpoint 'Microsoft.DataFactory/factories/managedVirtualNetworks/managedPrivateEndpoints@2018-06-01' = {
  parent: managedNetwork
  name: functionEndpointName
  properties: {
    groupId: 'sites'
    privateLinkResourceId: functionAppId
  }
}

resource vaultEndpoint 'Microsoft.DataFactory/factories/managedVirtualNetworks/managedPrivateEndpoints@2018-06-01' = {
  parent: managedNetwork
  name: vaultEndpointName
  properties: {
    groupId: 'vault'
    privateLinkResourceId: invocationVaultId
  }
}

output connectVia object = {
  referenceName: integrationRuntimeName
  type: 'IntegrationRuntimeReference'
}
output managedPrivateEndpointIds array = [functionEndpoint.id, vaultEndpoint.id]
