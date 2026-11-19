@description('Existing general-purpose v2 Azure Storage account in this resource group (separate from transfer data).')
param accountName string
param tableName string = 'HourlyCopyRecovery'
param reportContainerName string = 'hourly-copy-recovery'

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: accountName
}
resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' existing = {
  parent: storage
  name: 'default'
}
resource tableService 'Microsoft.Storage/storageAccounts/tableServices@2023-05-01' existing = {
  parent: storage
  name: 'default'
}
resource reports 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: reportContainerName
  properties: {publicAccess: 'None'}
}
resource ledger 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = {
  parent: tableService
  name: tableName
}
output reportContainerId string = reports.id
output recoveryTableId string = ledger.id
