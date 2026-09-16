# pgAssistant API v2 integration guide

pgAssistant API v2 exposes PostgreSQL analysis capabilities to external tools
without relying on a browser session. It is intended for automation platforms,
database portals, CI/CD workflows, infrastructure orchestrators, and other tools
participating in a continuous PostgreSQL improvement loop.

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/api/v2/pgtune` | Calculate a PostgreSQL configuration baseline. |
| `POST` | `/api/v2/executive-plan` | Generate a prioritized remediation plan from all supported advisors. |

These routes are independent from the historical `/api/v1` routes. Future
breaking integration changes will use another API version.

## API discovery

For a pgAssistant instance running on `http://localhost:8080`:

- Swagger UI: `http://localhost:8080/api/v2/docs`
- Swagger specification: `http://localhost:8080/api/v2/swagger.json`

Replace `localhost:8080` with the address of the pgAssistant deployment.

## Authentication

Authentication is controlled by the `PGA_API_TOKEN` environment variable:

- when it is unset or empty, API authentication is disabled;
- when it is set, every v2 operation requires the same value as a Bearer token.

Example Docker configuration:

```yaml
services:
  pgassistant:
    image: bertrand73/pgassistant:latest
    environment:
      PGA_API_TOKEN: ${PGA_API_TOKEN}
```

Clients then send:

```http
Authorization: Bearer your-secret-token
```

An absent or invalid token returns HTTP `401`:

```json
{
  "success": false,
  "error": "Missing or invalid API token."
}
```

For production, configure a strong token, terminate TLS in front of pgAssistant,
and store the token and database URI in a secrets manager. An unprotected API can
connect to any PostgreSQL address reachable from the pgAssistant container, so
disabled authentication should be limited to trusted development environments.

## Common conventions

Requests and responses use JSON. Clients should send:

```http
Content-Type: application/json
Accept: application/json
```

PostgreSQL connections use a standard libpq-compatible URI:

```text
postgresql://user:password@host:5432/database
```

Connection credentials are used only for the requested analysis. They are not
included in successful responses or public error details.

## pgTune API

### Endpoint

```http
POST /api/v2/pgtune
```

This endpoint supports explicit resource sizing and connected database sizing.
It generates recommendations but never applies them.

### Explicit resource mode

Omit `database_uri` and provide the resources to size:

```json
{
  "cpu": 4,
  "memory_mb": 8192,
  "postgresql_version": "18",
  "database_type": "web",
  "storage": "ssd",
  "max_connections": 100
}
```

| Field | Required | Description |
| --- | --- | --- |
| `cpu` | Yes | Positive number of CPUs. |
| `memory_mb` | Yes | Positive amount of memory in MiB. |
| `postgresql_version` | No | PostgreSQL major version; defaults to `18`. |
| `database_type` | No | `web`, `oltp`, or `dw`; defaults to `web`. |
| `storage` | No | `ssd`, `san`, or `hdd`; defaults to `ssd`. |
| `max_connections` | No | Positive connection limit; defaults to `100`. |

Example request:

```bash
curl --request POST 'http://localhost:8080/api/v2/pgtune' \
  --header "Authorization: Bearer ${PGA_API_TOKEN}" \
  --header 'Content-Type: application/json' \
  --data '{
    "cpu": 4,
    "memory_mb": 8192,
    "postgresql_version": "18",
    "database_type": "oltp",
    "storage": "ssd",
    "max_connections": 200
  }'
```

Example response:

```json
{
  "success": true,
  "source": "request",
  "inputs": {
    "cpu": 4,
    "memory_mb": 8192,
    "postgresql_version": "18",
    "database_type": "oltp",
    "storage": "ssd",
    "max_connections": 200
  },
  "recommendations": {
    "shared_buffers": "2GB",
    "effective_cache_size": "6GB",
    "maintenance_work_mem": "512MB"
  }
}
```

The recommendation set can evolve. Integrations should treat `recommendations`
as a parameter-to-value object instead of relying on a fixed parameter list.

### Connected database mode

Provide `database_uri` to detect the PostgreSQL version, available resources and
current settings:

```json
{
  "database_uri": "postgresql://user:password@postgres:5432/application",
  "database_type": "web",
  "storage": "ssd"
}
```

Explicit `cpu`, `memory_mb`, `postgresql_version`, and `max_connections` values
override detected values. This is useful when an external platform manages the
target resource limits.

Connected mode adds these response fields:

| Field | Description |
| --- | --- |
| `detected_resources` | Effective CPU, memory in MiB, and detected environment. |
| `current_values` | Current PostgreSQL parameters considered by pgTune. |
| `alter_system_sql` | SQL for recommended values that differ from current values. |

Resource detection reads Linux and cgroup information through PostgreSQL. The
database role generally needs superuser or `pg_read_server_files` privileges. If
resources cannot be inspected, the endpoint returns HTTP `422`.

Always review `alter_system_sql` before execution. Some settings require a restart
or must be adapted to the deployment platform.

## Executive Plan API

### Endpoint

```http
POST /api/v2/executive-plan
```

`database_uri` is required:

```json
{
  "database_uri": "postgresql://user:password@postgres:5432/application"
}
```

Example request:

```bash
curl --request POST 'http://localhost:8080/api/v2/executive-plan' \
  --header "Authorization: Bearer ${PGA_API_TOKEN}" \
  --header 'Content-Type: application/json' \
  --data '{
    "database_uri": "postgresql://user:password@postgres:5432/application"
  }'
```

The operation runs the Global, Index, Parameter, and Autovacuum advisors and
combines their findings into ordered work packages. It is read-only: recommended
SQL is returned but never executed.

### Response structure

The API uses a normalized representation so each recommendation occurs once:

```json
{
  "status": "ok",
  "database": "application",
  "phases": [
    {
      "number": 40,
      "name": "Improve query access paths",
      "task_ids": ["6d35348d4d"]
    }
  ],
  "tasks": [
    {
      "id": "6d35348d4d",
      "phase": 40,
      "title": "Consolidate index recommendations by table",
      "team": "DEV_OPS",
      "priority": "HIGH",
      "score": 82,
      "workstream": "INDEX_STRATEGY",
      "recommendation_count": 1,
      "query_ids": ["6536098310345248286"],
      "recommendations": [
        {
          "source": "index_advisor",
          "sources": ["index_advisor"],
          "advisor_id": "filter_index",
          "action_type": "CREATE_INDEX",
          "scope_name": "public.orders",
          "title": "Review index opportunity on public.orders",
          "description": "A selective predicate can use this index.",
          "sql": "CREATE INDEX CONCURRENTLY ON public.orders (customer_id);",
          "query_ids": ["6536098310345248286"]
        }
      ]
    }
  ],
  "errors": [],
  "summary": {
    "recommendations_collected": 1,
    "recommendations_after_deduplication": 1,
    "tasks": 1,
    "phases": 1
  },
  "postgres_context": {
    "available": true,
    "server_version": "18.3",
    "major_version": 18,
    "settings": {}
  }
}
```

Use `phases[].task_ids` to obtain ordered tasks from the canonical top-level
`tasks` array. Do not infer ordering from task IDs.

Important recommendation fields include:

| Field | Description |
| --- | --- |
| `source` | Advisor that originally produced the recommendation. |
| `sources` | All advisors merged into it during deduplication. |
| `action_type` | Suggested action, such as `CREATE_INDEX` or `CONFIG_CHANGE`. |
| `team` | Suggested owner: `DEV`, `OPS`, or `DEV_OPS`. |
| `priority` | Recommendation priority. |
| `sql` | Recommended SQL when available; otherwise empty. |
| `query_ids` | PostgreSQL query IDs that directly led to the recommendation. |

An empty `query_ids` list is valid. A missing foreign-key index, for example, may
be detected from the schema without being triggered by an observed statement. If
the Index Advisor independently identifies the same action, deduplication merges
its query IDs and adds `index_advisor` to `sources`.

The `errors` array contains advisor-level failures. pgAssistant can return
`status: "ok"` with a useful partial plan when one advisor is unavailable, so
integrations should inspect both `status` and `errors`.

## HTTP status codes

| Status | Meaning |
| --- | --- |
| `200` | Analysis completed; inspect response and advisor status fields. |
| `400` | Invalid JSON, missing field, unsupported value, or invalid URI. |
| `401` | The configured Bearer token is absent or invalid. |
| `422` | The database cannot be reached or resources cannot be inspected. |
| `500` | An unexpected analysis failure occurred. |

Error responses use this shape:

```json
{
  "success": false,
  "error": "Human-readable error message."
}
```

Use the HTTP status code for control flow and treat the message as diagnostic
text, not as a stable machine identifier.

## Suggested integration workflow

1. Call `/api/v2/executive-plan` to collect and prioritize recommendations.
2. Present proposed actions and SQL for human or policy approval.
3. Apply approved changes through the external operational platform.
4. Collect new workload measurements with pgAssistant Collector.
5. Compare the next measurement with the previous state to verify impact.

`/api/v2/pgtune` can provide a configuration baseline before generating the
Executive Plan. Neither endpoint executes returned SQL, keeping diagnosis and
execution as distinct control points.

## Compatibility recommendations

- Pin integrations to the `/api/v2` prefix.
- Ignore unknown response fields to support additive API changes.
- Treat PostgreSQL query IDs as strings; signed 64-bit values are not safe in all
  JavaScript numeric representations.
- Do not assume every recommendation contains SQL or query IDs.
- Use a suitable timeout: Executive Plan generation invokes several advisors.
- Never execute `sql` or `alter_system_sql` without approval and safety checks.
