## beat output http plugin

### Static path mode

Legacy behavior stays unchanged when only `path` is configured.

```yaml
output.http:
  hosts: ["http://litelog:12100"]
  path: /litelog-new/api/log/stream/edge_app
  batch_publish: true
```

### Dynamic stream mode

When `path_prefix` and `path_field` are both configured, the plugin reads the
target stream from each event field and appends it to the prefix at publish time.
The routing field is used only to select the request path and is not serialized
into the outbound HTTP body.

```yaml
output.http:
  hosts: ["http://litelog:12100"]
  path_prefix: /litelog-new/api/log/stream
  path_field: stream
  batch_publish: true
```

Examples:

- `stream=edge_app` -> `/litelog-new/api/log/stream/edge_app`
- `stream=edge_k3s` -> `/litelog-new/api/log/stream/edge_k3s`

#### Optional path suffix

Use `path_suffix` to append a fixed suffix after the dynamic stream value:

```yaml
output.http:
  hosts: ["http://litelog:12100"]
  path_prefix: /litelog-new/api/log/stream
  path_field: stream
  path_suffix: /_json
  batch_publish: true
```

Examples:

- `stream=edge_app` -> `/litelog-new/api/log/stream/edge_app/_json`
- `stream=edge_k3s` -> `/litelog-new/api/log/stream/edge_k3s/_json`

### Batch behavior

When `batch_publish: true` is enabled in dynamic mode, events in the same batch
are grouped by resolved target path and sent in separate HTTP requests.

### Fallback behavior

If dynamic mode is enabled but an event does not contain the configured
`path_field`, the plugin falls back to the static `path` when one is configured.
If neither a dynamic field nor a static `path` is available, publishing fails.

`http://host` and `http://host/` are both treated as "no static path fallback":
they do not provide a recoverable stream path, so missing `path_field` will fail
instead of silently posting to the host root.
