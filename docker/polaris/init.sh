#!/bin/sh
set -e

POLARIS=http://polaris:8181

echo "==> Fetching bootstrap token..."
TOKEN=$(curl -sf -X POST "${POLARIS}/api/catalog/v1/oauth/tokens" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL" \
  | grep -o '"access_token":"[^"]*"' | cut -d'"' -f4)

echo "==> Creating catalog demo_lh..."
curl -sf -X POST "${POLARIS}/api/management/v1/catalogs" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": {
      "name": "demo_lh",
      "type": "INTERNAL",
      "properties": { "default-base-location": "s3://iceberg-warehouse/" },
      "storageConfigInfo": {
        "storageType": "S3",
        "allowedLocations": ["s3://iceberg-warehouse/"],
        "roleArn": "arn:aws:iam::000000000000:role/polaris",
        "region": "us-east-1",
        "endpoint": "http://minio:9000",
        "endpointInternal": "http://minio:9000",
        "pathStyleAccess": true,
        "stsUnavailable": true,
        "kmsUnavailable": true
      }
    }
  }'

echo ""
echo "==> Creating catalog role data-role..."
curl -sf -X POST "${POLARIS}/api/management/v1/catalogs/demo_lh/catalog-roles" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole": {"name": "data-role"}}'

echo ""
echo "==> Granting CATALOG_MANAGE_CONTENT to data-role..."
curl -sf -X PUT "${POLARIS}/api/management/v1/catalogs/demo_lh/catalog-roles/data-role/grants" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}'

echo ""
echo "==> Creating principal role data-access..."
curl -sf -X POST "${POLARIS}/api/management/v1/principal-roles" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"principalRole": {"name": "data-access"}}'

echo ""
echo "==> Assigning data-role to data-access..."
curl -sf -X PUT "${POLARIS}/api/management/v1/principal-roles/data-access/catalog-roles/demo_lh" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole": {"name": "data-role"}}'

echo ""
echo "==> Assigning root principal to data-access..."
curl -sf -X PUT "${POLARIS}/api/management/v1/principals/root/principal-roles" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"principalRole": {"name": "data-access"}}'

echo ""
echo "==> Polaris bootstrap complete."
