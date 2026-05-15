INSTALL httpfs;
INSTALL iceberg;
LOAD httpfs;
LOAD iceberg;

CREATE OR REPLACE SECRET minio (
    TYPE        s3,
    KEY_ID      'minioadmin',
    SECRET      'minioadmin',
    ENDPOINT    'minio:9000',
    URL_STYLE   'path',
    USE_SSL     false,
    REGION      'us-east-1'
);

ATTACH 'demo_lh' AS iceberg_catalog (
    TYPE                   iceberg,
    ENDPOINT               'http://polaris:8181/api/catalog',
    AUTHORIZATION_TYPE     'oauth2',
    CLIENT_ID              'root',
    CLIENT_SECRET          's3cr3t',
    OAUTH2_SERVER_URI      'http://polaris:8181/api/catalog/v1/oauth/tokens',
    SCOPE                  'PRINCIPAL_ROLE:ALL',
    ACCESS_DELEGATION_MODE 'none'
);
