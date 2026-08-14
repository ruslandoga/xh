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
deadline covers the pool checkout and network work. Queries are never retried
internally.

`Xh.query/2` buffers the complete response in memory. This initial transport
targets small insert acknowledgements; it is not intended for large query
results.

Named query parameters are passed as the third argument. The fourth argument
accepts `:headers`, ClickHouse `:settings`, and a `:timeout` that defaults to 30
seconds. Statements may be iodata, so encoded insert rows can follow the SQL
without first being concatenated into a new binary.

Pool options include `:name`, `:url`, `:pool_size`, `:worker_idle_timeout`, and
`:transport_opts` passed to Mint. A URL path prefixes each query target, while
the scheme, host, and port bind the pool to a single origin.
