"""Local contract checks; Azure routing and Bicep deployment require staging."""

# ----------------------------------------------------------------------------
# TEST MODULE - test_private_profile.py
# Static consistency checks for the optional Private Link profile in infra/private-link/.
# They compare JSON and Bicep text only; real network routing must be tested in Azure.
# Run all tests from the project root: python -m unittest discover -s tests -v
# ----------------------------------------------------------------------------
import json
import re
import unittest
from pathlib import Path

# Project root and the Private Link profile folder.
ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / 'infra' / 'private-link'


# Helpers: load a JSON file; walk_activities() finds every Azure Function activity.
def read_json(path):
    return json.loads(path.read_text(encoding='utf-8'))


# Recursively yield every AzureFunctionActivity in a pipeline document.
def walk_activities(document):
    if isinstance(document, dict):
        if document.get('type') == 'AzureFunctionActivity':
            yield document
        for value in document.values():
            yield from walk_activities(value)
    elif isinstance(document, list):
        for item in document:
            yield from walk_activities(item)


# The private linked service differs from the standard one only by connectVia; both pipeline
# HTTP activities use it; the integration runtime and DNS zone names match the samples.
class PrivateDeploymentContractTests(unittest.TestCase):
    def test_private_linked_service_only_changes_routing(self):
        base = read_json(ROOT / 'adf' / 'ls_hourly_copy_function.json')
        private = read_json(PROFILE / 'ls_hourly_copy_function.private.example.json')
        route = private['properties'].pop('connectVia')
        self.assertEqual(route['type'], 'IntegrationRuntimeReference')
        self.assertEqual(private, base)

    def test_both_http_activities_use_the_private_linked_service_name(self):
        pipeline = read_json(ROOT / 'adf' / 'pl_hourly_blob_copy.json')
        private = read_json(PROFILE / 'ls_hourly_copy_function.private.example.json')
        activities = {activity['name']: activity for activity in walk_activities(pipeline)}
        self.assertEqual(set(activities), {'StartCopy', 'GetCopyStatus'})
        for activity in activities.values():
            self.assertEqual(activity['linkedServiceName']['referenceName'], private['name'])

    def test_default_runtime_name_matches_the_linked_service_and_sample_parameters(self):
        private = read_json(PROFILE / 'ls_hourly_copy_function.private.example.json')
        name = private['properties']['connectVia']['referenceName']
        for filename, declaration in (
            ('adf-private-access.bicep', 'param integrationRuntimeName string'),
            ('adf-private-access.example.bicepparam', 'param integrationRuntimeName'),
        ):
            with self.subTest(filename=filename):
                source = (PROFILE / filename).read_text(encoding='utf-8')
                self.assertIn(f"{declaration} = '{name}'", source)

    def test_default_dns_zone_keys_match_the_workload_example(self):
        defaults = (PROFILE / 'private-dns.bicep').read_text(encoding='utf-8')
        sample = (PROFILE / 'workload-endpoints.example.bicepparam').read_text(encoding='utf-8')
        base_zones = dict(re.findall(r"^  (\w+): '(privatelink\.[^']+)'", defaults, re.M))
        sample_zones = dict(re.findall(r"^  (\w+): '\$\{dnsPrefix\}/(privatelink\.[^']+)'", sample, re.M))
        self.assertEqual(set(base_zones), {'blob', 'queue', 'table', 'acr'})
        self.assertEqual(base_zones, sample_zones)


if __name__ == '__main__':
    unittest.main()
