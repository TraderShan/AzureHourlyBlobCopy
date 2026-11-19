// Deploy in the existing AMPLS resource group. Does not change scope modes or public network access flags.
@minLength(1)
param scopeName string
@minLength(1)
param applicationInsightsId string
@minLength(1)
param logAnalyticsWorkspaceId string
param applicationInsightsAssociationName string = 'hourly-copy-app-insights'
param workspaceAssociationName string = 'hourly-copy-log-analytics'

resource scopeResource 'Microsoft.Insights/privateLinkScopes@2023-06-01-preview' existing = {
  name: scopeName
}

resource appInsights 'Microsoft.Insights/privateLinkScopes/scopedResources@2023-06-01-preview' = {
  parent: scopeResource
  name: applicationInsightsAssociationName
  properties: { linkedResourceId: applicationInsightsId }
}

resource workspace 'Microsoft.Insights/privateLinkScopes/scopedResources@2023-06-01-preview' = {
  parent: scopeResource
  name: workspaceAssociationName
  properties: { linkedResourceId: logAnalyticsWorkspaceId }
}

output monitorScopeId string = scopeResource.id
