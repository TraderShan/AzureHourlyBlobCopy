@description('Full Azure resource ID of the existing Data Factory.')
param dataFactoryId string

@description('Name of the hourly copy pipeline in that factory.')
param pipelineName string = 'pl_hourly_blob_copy'

@description('Full resource ID of an existing Azure Monitor action group.')
param actionGroupId string

param alertName string = '${pipelineName}-failed-runs'
@minValue(0)
@maxValue(4)
param severity int = 2
param enabled bool = false

resource failedPipelineAlert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: alertName
  location: 'global'
  properties: {
    description: 'A copy pipeline run failed. Review ADF activity details and correlate with Function telemetry.'
    enabled: enabled
    severity: severity
    scopes: [dataFactoryId]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT15M'
    autoMitigate: true
    criteria: {
      'odata.type': 'Microsoft.Azure.Monitor.SingleResourceMultipleMetricCriteria'
      allOf: [
        {
          criterionType: 'StaticThresholdCriterion'
          name: 'FailedPipelineRuns'
          metricNamespace: 'Microsoft.DataFactory/factories'
          metricName: 'PipelineFailedRuns'
          timeAggregation: 'Total'
          operator: 'GreaterThan'
          threshold: 0
          dimensions: [{name: 'PipelineName', operator: 'Include', values: [pipelineName]}]
        }
      ]
    }
    actions: [{actionGroupId: actionGroupId}]
  }
}
