# Private Link deployment profile

Use this profile to keep the copy workload's runtime traffic on private Azure paths. The Function code, Azure Data Factory (ADF) pipeline activities and tumbling-window schedule do not change. The profile adds an optional linked service variant and Bicep for endpoint and DNS provisioning; it **does not** create the Function App, VNet/subnets, registry, vault, Storage, or Azure Monitor resources. The templates do not set public-access or firewall properties, grant roles, publish ADF resources, or start triggers.

**Existing Function App:** Check `publicNetworkAccess` before creating the first inbound private endpoint (including an ADF-managed one). If the property is unset, adding a private endpoint can [change the default from enabled to disabled](https://learn.microsoft.com/en-us/azure/app-service/overview-access-restrictions#app-access). Treat this as a cutover, not a harmless pre-stage: pause and drain existing callers before creating that endpoint if they still use the public path. Prove the replacement path before resuming them.

| Connection | Where the private endpoint goes |
|---|---|
| ADF managed Azure integration runtime → Function HTTP start and status | ADF managed VNet: Function App `sites` endpoint |
| ADF managed Azure integration runtime → Key Vault for the invocation key | ADF managed VNet: Key Vault `vault` endpoint |
| Function → source and destination Storage | Customer VNet: `blob` for each account; put both Blob endpoints in the VNet used for Function outbound integration when possible |
| Function → Durable state and Functions host storage | Customer VNet: `blob`, `queue`, `table`; add `file` if an Azure Files content share is used |
| Function → recovery reports and ledger, if enabled | Customer VNet: `blob`, `table` |
| Function → Key Vault, if the Function reads SAS or other vault secrets | Customer VNet: separate `vault` endpoint, even if it is the same vault ADF uses |
| Function and build agent → Premium Azure Container Registry (ACR) | Customer VNet or approved connected build network: `registry` endpoint with private DNS for registry login and data endpoints |
| Function telemetry and operator log queries → Azure Monitor | Customer/access VNet: `azuremonitor` endpoint for an Azure Monitor Private Link Scope (AMPLS) containing both Application Insights and its Log Analytics workspace |
| Private operator or deployment access directly to the Function App (optional) | Customer VNet: a separate Function `sites` endpoint; the ADF-managed endpoint is not reachable from an operator VNet |

### 1. Prepare the networks, DNS, and identities

- Use a **Linux Elastic Premium** Function App for this profile and an ADF managed VNet Azure IR. No self-hosted IR is needed for the supplied pattern. A private endpoint **on ADF** is a different feature for inbound access (for example, self-hosted IR command traffic or private portal access); it does not make the Function calls private. See [ADF managed VNet and managed private endpoints](https://learn.microsoft.com/en-us/azure/data-factory/managed-virtual-network-private-endpoint) and [ADF's Function linked service walkthrough](https://techcommunity.microsoft.com/blog/azuredatafactoryblog/azure-functions-available-in-adf-managed-virtual-network/3298383).
- Enable [regional outbound VNet integration](https://learn.microsoft.com/en-us/azure/azure-functions/functions-networking-options) on a **separate** subnet delegated to `Microsoft.Web/serverFarms`. The integration subnet is not the private endpoint subnet. Size the endpoint subnet for all endpoints, including the multiple IPs consumed by ACR and AMPLS. Validate routes and firewall policies. If routing all outbound traffic through a firewall, allow required Azure identity and platform dependencies under your policy.
- Use the normal hostnames (for example, `example.blob.core.windows.net` and `example.azurewebsites.net`) in AzCopy, app settings, and ADF. Do not substitute a private IP or `privatelink` hostname. Link the zones below to the VNet(s) that need them, or configure forwarding to your approved Azure DNS resolver. ADF manages DNS for its own managed endpoints; do not include its managed VNet in the customer DNS zone links.
- Keep the role assignments from [the main README](../../README.md#identity-key-vault-and-networking). Add the Function's image-pull identity to ACR with the appropriate pull role for the registry's RBAC mode, give the build agent separate push permissions, and retain the ADF identity's permission to read the invocation secret. Private Link is a network control, not authentication.

### 2. Provision the customer-VNet endpoints

Use `workload-endpoints.bicep` in a resource group in the endpoint subnet's subscription. Set `location` to that VNet's region. Edit a copy of `workload-endpoints.example.bicepparam`; supply full resource IDs, including cross-resource-group IDs where appropriate. The template adds DNS zone groups to manage private records. Use `requestManualApproval=true` if endpoint connections must be approved by target resource owners. Reuse connections already managed by your platform team; don't deploy a duplicate endpoint for the same target into the same DNS view. In an existing deployment with some endpoints already managed elsewhere, use the template for a dedicated staging environment or integrate the missing connections into the owner's IaC rather than running the full workload template against shared resources.

| Key in `privateDnsZoneIds` | Azure public-cloud private DNS zone | Needed when |
|---|---|---|
| `blob` | `privatelink.blob.core.windows.net` | Always; reused by AMPLS if configured |
| `queue` | `privatelink.queue.core.windows.net` | Always |
| `table` | `privatelink.table.core.windows.net` | Always |
| `acr` | `privatelink.azurecr.io` | Always |
| `file` | `privatelink.file.core.windows.net` | `useHostFileShare=true` |
| `vault` | `privatelink.vaultcore.azure.net` | `functionVaultId` is set |
| `sites` | `privatelink.azurewebsites.net` | `operatorFunctionAppId` is set |
| `monitor` | `privatelink.monitor.azure.com` | `monitorScopeId` is set |
| `oms` | `privatelink.oms.opinsights.azure.com` | `monitorScopeId` is set |
| `ods` | `privatelink.ods.opinsights.azure.com` | `monitorScopeId` is set |
| `agentsvc` | `privatelink.agentsvc.azure-automation.net` | `monitorScopeId` is set |

Use existing enterprise private DNS zones where available. For a greenfield single VNet, `private-dns.bicep` can create and link the four base zones; override `zoneNames` with the full map (base plus enabled optional entries) as needed. Its `privateDnsZoneIds` output supplies the input map for `workload-endpoints.bicep`. Coordinate any additional VNet/forwarding links with the DNS owner. The optional `sites` zone must also resolve the app's SCM hostname if your deployment mechanism uses SCM/Kudu. Private endpoints are per slot if you use deployment slots; the supplied ADF and operator examples target the app, not a slot.

Configure the Function to pull the private ACR image with managed identity (`siteConfig.acrUseManagedIdentityCreds=true`; also set `siteConfig.acrUserManagedIdentityID` to the client ID if using a user-assigned pull identity). [Route the image pull over VNet integration](https://learn.microsoft.com/en-us/azure/app-service/configure-vnet-integration-routing) with `outboundVnetRouting.imagePullTraffic=true`, or use `outboundVnetRouting.allTraffic=true` when centrally routing all outbound traffic. If using an Azure Files share, provision it beforehand, configure content-share routing, and allow the required HTTPS (443) and SMB (445) traffic. Check both the registry login and regional data names resolve privately. Build and push from an agent with network access to that ACR. If the build environment also prohibits public egress, mirror the approved Functions base image and Python packages to internal sources and provide the pinned, verified AzCopy binary offline. The stock Dockerfile pulls the base image from MCR and installs dependencies at build time; this is not an air-gapped build.

### 3. Configure ADF and monitoring

1. Deploy `adf-private-access.bicep` to the existing factory's resource group using a copy of `adf-private-access.example.bicepparam`. If reusing a managed VNet IR, set `deployIntegrationRuntime=false` and specify that IR's name. Align the endpoint names with existing connections or reuse approved connections rather than duplicating them. Approve both managed private endpoint requests on the Function App and the invocation vault; confirm `Approved` in ADF. The staging linked-service switch must wait for approval.
2. Apply `ls_hourly_copy_function.private.example.json` as the existing `ls_hourly_copy_function` resource through your normal ADF Git/CI process. Keep the name so it serves both `StartCopy` and `GetCopyStatus`, including runs invoked from the lookback pipeline. Replace the host and secret-name placeholders as needed, and set `properties.connectVia.referenceName` to the deployed IR. Set the invocation vault URL in `adf/ls_hourly_key_vault.json` as before. The private variant keeps the Function key reference; a successful ADF **Key Vault linked-service test alone only validates the URL format**, not private secret retrieval. Test a pipeline run with the vault's public network access disabled in staging. Do not add the host key to pipeline JSON, command lines, or query strings.
3. Reuse the approved AMPLS for a shared DNS topology. For an isolated DNS environment without one, create a scope following the [Azure Monitor private link guide](https://learn.microsoft.com/en-us/azure/azure-monitor/fundamentals/private-link-configure), then use `monitor-scope-resources.bicep` in its resource group to associate the Application Insights component and the Log Analytics workspace. If the network can already reach that scope's endpoint, leave `monitorScopeId` unset in the workload template; otherwise set it and use all five AMPLS DNS zones. Keep existing ingestion/query settings during setup. Confirm all required resources using shared DNS are in scope before considering `PrivateOnly` and disabling public ingestion/query on the component and workspace. Operators need a network path to the private query endpoint. Test the ADF failed-run metric separately from log-based alerts and notifications.

Example validation commands (run from this directory with your approved Azure CLI and the edited parameter files):

```bash
az bicep build --file private-dns.bicep
az bicep build --file workload-endpoints.bicep
az bicep build --file adf-private-access.bicep
az bicep build --file monitor-scope-resources.bicep
az deployment group what-if --resource-group "$NETWORK_RG" --parameters workload-endpoints.stage.bicepparam
az deployment group what-if --resource-group "$ADF_RG" --parameters adf-private-access.stage.bicepparam
```

Use reviewed incremental deployments in staging. These modules do not publish ADF resources or enforce public network restrictions. Manage public access on each resource separately after its private paths are in place; check for existing producers, consumers, or shared tooling before changing a firewall. Private Link secures supported data paths. It does not make Microsoft Entra, Azure Resource Manager, public package feeds, the Azure portal, or alert recipients private by itself.

### 4. Staging acceptance checklist

- [ ] Verify every expected endpoint is approved. Check that the Function worker, build agent and operator client resolve the normal hostnames to the expected private addresses. Check TLS/HTTPS succeeds without bypassing certificate validation. Confirm access from an unauthorized/public network is denied once the staging resources' public network paths are disabled.
- [ ] Restart or redeploy the staging Function using a fresh image tag. Confirm a real pull from private ACR, Functions host startup, Durable state access, and timer operation if recovery is enabled. Confirm both ADF start and polling succeed, using the configured managed IR and the private invocation vault. Do not count a linked-service test as the end-to-end test.
- [ ] With both storage accounts' public paths disabled, run the deployed Function with the pinned AzCopy binary against a real closed hour and verify the contents. Test both the normal copy and reconciliation with a nested file. In strict mode, confirm `_SUCCESS` and the verification event. Do **not** assume successful listings prove the server-to-server leg.
- [ ] Simulate a partial failure in staging with a controlled retryable condition. Verify the private failure report and Table row when recovery is enabled. Confirm recovery sends only missing files and does not overwrite conflicts. A permission or topology failure is not a reason to increase retry counts.
- [ ] Verify `AppTraces` ingestion, the operations KQL queries from the operator network, an ADF failed-run alert, and (for strict continuous feeds) an overdue verification alert. Confirm the action group reaches its approved destination. Do not assume AMPLS makes email or a webhook private.
- [ ] Before production cutover, save the prior linked-service and approved network configuration, pause both applicable ADF triggers, and let active runs drain. Coordinate with other Function callers before adding the first inbound endpoint or changing DNS and public access. If changing dependencies used by automatic recovery, also pause its dispatch and confirm in-flight recovery has finished. If staging cannot meet the requirements, leave production as-is; do not temporarily expose storage to make the transfer succeed. Resume scheduling only after the cutover checks pass.

**AzCopy topology troubleshooting:** A hub Function may reach both storage names while a server-side transfer across isolated spokes fails with `403 CannotVerifyCopySource`. Prefer co-locating the two Blob private endpoints in the Function's integration VNet; for a different design, validate the [documented supported topologies and remedies](https://learn.microsoft.com/en-us/troubleshoot/azure/azure-storage/blobs/connectivity/copy-blobs-between-storage-accounts-network-restriction) with your Azure networking team. Check the real copy before considering a different transfer architecture. Do not add a public firewall bypass or fall back to staging file contents on local disk automatically.
