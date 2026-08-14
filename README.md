# Xh

Xh is a small ClickHouse HTTP ingestion transport. It provides a lazy,
single-origin pool of passive Mint HTTP/1 connections:

```elixir
{:ok, pool} = Xh.start_link(url: "http://localhost:8123")

{:ok, 200, headers, body} =
  Xh.query(pool, {"POST", "/", [], "INSERT INTO events FORMAT RowBinary\n..."}, 5_000)
```

Connections are opened only when a query checks one out, reused one query at a
time, and removed after timeouts, transport failures, or closure. One absolute
deadline covers the pool checkout and network work. Queries are never retried
internally.

`Xh.query/3` buffers the complete response in memory. This initial transport
targets small insert acknowledgements; it is not intended for large query
results.

Pool options include `:name`, `:url`, `:pool_size`, `:worker_idle_timeout`, and
`:transport_opts` passed to Mint. A URL path prefixes each query target, while
the scheme, host, and port bind the pool to a single origin.
