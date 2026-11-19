@description('Resource ID of the Log Analytics workspace used by Application Insights.')
param workspaceId string
@description('Resource ID of the workspace-based Application Insights component.')
param applicationInsightsId string
@description('AppRoleName exactly as observed in AppTraces; scope to a single production Function App.')
param functionRoleName string
@description('First UTC window start to monitor, for example 2026-11-18T16:00:00Z; use a top-of-hour value.')
param firstExpectedWindowUtc string
@description('Minutes after each UTC window ends, including trigger delay, copy time and telemetry ingestion.')
@minValue(5)
@maxValue(720)
param graceMinutes int = 90
@description('Number of recent due windows to check for gaps.')
@minValue(1)
@maxValue(24)
param horizonHours int = 12
@description('Existing Azure Monitor action group resource ID.')
param actionGroupId string
param alertName string = 'hourly-blob-copy-verification-overdue'
param location string = resourceGroup().location
@minValue(0)
@maxValue(4)
param severity int = 2
param enabled bool = false

var normalizedStart = dateTimeAdd(firstExpectedWindowUtc, 'PT0S', 'yyyy-MM-ddTHH:mm:ssZ')
var query = replace(replace(replace(replace(replace(loadTextContent('verification-overdue.kql'), '__START__', base64(normalizedStart)), '__ROLE__', base64(functionRoleName)), '__INSIGHTS__', base64(applicationInsightsId)), '__GRACE__', string(graceMinutes)), '__HORIZON__', string(horizonHours))

resource deadlineAlert 'Microsoft.Insights/scheduledQueryRules@2023-12-01' = {
  name: alertName
  location: location
  kind: 'LogAlert'
  properties: {
    displayName: alertName
    description: 'At least one expected UTC window has no manifest-verified completion by its deadline.'
    enabled: enabled
    severity: severity
    scopes: [workspaceId]
    evaluationFrequency: 'PT15M'
    windowSize: 'PT15M'
    overrideQueryTimeRange: 'P2D'
    autoMitigate: true
    skipQueryValidation: false
    criteria: {
      allOf: [
        {
          query: query
          operator: 'GreaterThan'
          threshold: 0
          timeAggregation: 'Count'
          failingPeriods: {
            minFailingPeriodsToAlert: 1
            numberOfEvaluationPeriods: 1
          }
        }
      ]
    }
    actions: {actionGroups: [actionGroupId]}
  }
}
