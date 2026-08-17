# Xh

Xh is a small ClickHouse HTTP ingestion transport. It provides a lazy,
single-origin pool of passive Mint HTTP/1 connections:

```elixir
{:ok, pool} = Xh.start_link(url: "http://localhost:8123")

{:ok, 200, headers, body} =
  Xh.query(pool, "INSERT INTO events FORMAT CSV\n1,hello\n")
```

Connections are opened only when a query checks one out, reused one query at a
time, and removed after timeouts, transport failures, or closure. One absolute
deadline covers pool checkout, connection establishment, and response receipt;
request transmission follows Mint's transport behavior. Queries are never
retried internally.

A pool-checkout timeout exits the caller. It is deliberately not caught and
converted into an error tuple, which could leave a racing checkout reply in the
caller's mailbox.

`Xh.query/2` buffers the complete response in memory. This initial transport
targets small insert acknowledgements; it is not intended for large query
results.

Named query parameters are passed as the third argument. The fourth argument
accepts `:headers`, ClickHouse `:settings`, and a `:timeout` that defaults to 30
seconds. Statements may be iodata, so encoded insert rows can follow the SQL
without first being concatenated into a new binary.

Pool options include `:name`, `:url`, `:max_conns`, `:worker_idle_timeout`, and
`:transport_opts` passed to Mint. The URL is an unauthenticated HTTP(S) origin;
non-root paths, userinfo, query strings, and fragments are rejected.
