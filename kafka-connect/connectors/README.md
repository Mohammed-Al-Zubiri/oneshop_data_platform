# Kafka Connect Connector Configurations

These JSON files are **local development configurations** that are submitted to the Kafka Connect REST API by `make setup-cdc` (via `make connectors`).

## Credentials Notice

The `database.password` field in these files uses the same default credentials as `.env.example`:

```
database.user:     postgresuser
database.password: postgrespw   ← matches POSTGRES_PASSWORD in .env.example
```

These are **intentional local-dev defaults** designed to work out of the box without any configuration. They match the values in `.env.example` and are not production secrets. If you change `POSTGRES_PASSWORD` in your `.env`, you must also update `database.password` in these connector JSON files.

> **Note:** Kafka Connect does not natively support environment variable interpolation in connector configs submitted via REST API. In a production setup, you would use [Kafka Connect Secret Providers](https://docs.confluent.io/platform/current/connect/security.html#externalizing-secrets) (e.g., `FileConfigProvider` or HashiCorp Vault) to avoid storing credentials in plaintext connector configs.

## Files

| File | Connector | Monitors |
|:-----|:----------|:---------|
| `items-connector.json` | `oneshop-postgres-items-connector` | `public.items` table → `oneshop.public.items` Kafka topic |
| `purchases-connector.json` | `oneshop-postgres-purchases-connector` | `public.purchases` table → `oneshop.public.purchases` Kafka topic |
| `opensearch-sink-connector.json` | `oneshop-opensearch-sink-connector` | `oneshop.public.items` topic → OpenSearch `items` index |

## Usage

```bash
# Register all connectors at once
make setup-cdc

# Or individually
make connectors

# Check status
make connectors-status
```
