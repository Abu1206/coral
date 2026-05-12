# Notion source

This bundled source queries Notion's read APIs with OAuth or an internal
integration token.

## Configure

### OAuth

Create a public Notion connection, configure this OAuth redirect URI, and copy
the OAuth client ID and client secret:

```text
http://127.0.0.1:53682/oauth/callback
```

Then run:

```sh
coral source add --interactive notion
```

Choose **Connect with Notion** and enter the OAuth client values when prompted.

### Internal integration token

Create a Notion internal integration, copy its internal integration token, and
share the pages or databases you want to query with that integration.

```sh
export NOTION_API_KEY="ntn_..."
coral source add notion
```

## Start querying

Discover shared pages and data sources:

```sql
SELECT id, object, url
FROM notion.search
LIMIT 20;
```

Inspect a data source schema:

```sql
SELECT name, id, type
FROM notion.data_source_properties
WHERE data_source_id = '...';
```

Query pages from a data source:

```sql
SELECT id, url, created_time, last_edited_time, properties
FROM notion.data_source_pages
WHERE data_source_id = '...'
LIMIT 100;
```
