// Optional for greenfield networking. Do not use to duplicate enterprise zones.
@minLength(1)
param virtualNetworkId string
@description('Map from workload-endpoints.bicep zone keys to public Azure private DNS zone names. Include optional keys for enabled features.')
param zoneNames object = {
  blob: 'privatelink.blob.core.windows.net'
  queue: 'privatelink.queue.core.windows.net'
  table: 'privatelink.table.core.windows.net'
  acr: 'privatelink.azurecr.io'
}

var zones = items(zoneNames)
resource privateZones 'Microsoft.Network/privateDnsZones@2020-06-01' = [for zone in zones: {
  name: zone.value
  location: 'global'
}]
resource vnetLinks 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = [for (zone, i) in zones: {
  parent: privateZones[i]
  name: 'link-${uniqueString(toLower(virtualNetworkId))}'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: { id: virtualNetworkId }
  }
}]

output privateDnsZoneIds object = toObject(zones, zone => zone.key, zone => resourceId('Microsoft.Network/privateDnsZones', zone.value))
