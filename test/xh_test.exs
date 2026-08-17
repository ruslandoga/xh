defmodule XhTest do
  use ExUnit.Case, async: true

  @query_timeout 1_000
  @clickhouse_url "http://localhost:8123"
  @client_port_query "SELECT port FROM system.processes WHERE query_id=currentQueryID() FORMAT TabSeparated"

  test "real ClickHouse queries reuse the same HTTP connection" do
    pool =
      start_supervised!(
        {Xh, url: @clickhouse_url, max_conns: 1, worker_idle_timeout: :infinity},
        id: make_ref()
      )

    first_port = clickhouse_client_port(pool)
    second_port = clickhouse_client_port(pool)

    assert second_port == first_port
  end

  test "a connection closed by real ClickHouse is replaced" do
    pool =
      start_supervised!(
        {Xh, url: @clickhouse_url, max_conns: 1, worker_idle_timeout: :infinity},
        id: make_ref()
      )

    first_port = clickhouse_client_port(pool)

    assert {:ok, 200, headers, body} =
             Xh.query(pool, @client_port_query, %{},
               headers: [{"connection", "close"}],
               timeout: @query_timeout
             )

    assert String.trim(body) == first_port
    assert {_, connection} = List.keyfind(headers, "connection", 0)
    assert String.downcase(connection) == "close"
    refute clickhouse_client_port(pool) == first_port
  end

  test "a timed-out real ClickHouse query is removed before the next query" do
    pool =
      start_supervised!(
        {Xh, url: @clickhouse_url, max_conns: 1, worker_idle_timeout: :infinity},
        id: make_ref()
      )

    first_port = clickhouse_client_port(pool)

    assert {:error, %Mint.TransportError{reason: :timeout}} =
             Xh.query(pool, "SELECT sleep(0.2)", %{}, timeout: 20)

    refute clickhouse_client_port(pool) == first_port
  end

  test "the pool is lazy and a second query reuses its connection" do
    test_process = self()

    handler = fn connection_id, request ->
      send(test_process, {:request, self(), connection_id, request})

      receive do
        {:respond, ^connection_id, body} -> {:respond, 200, [{"x-test", "yes"}], body}
      end
    end

    {server, url} = start_server(handler)

    pool =
      start_supervised!(
        {Xh,
         url: url, max_conns: 1, worker_idle_timeout: :infinity, transport_opts: [nodelay: true]}
      )

    refute_receive {:accepted, ^server, _connection_id}, 50

    first =
      query_async(pool, ["INSERT INTO events FORMAT CSV\n" | "1,first\n"], settings: [batch: 1])

    assert_receive {:accepted, ^server, 1}

    assert_receive {:request, ^server, 1,
                    %{
                      method: "POST",
                      target: "/?batch=1",
                      body: "INSERT INTO events FORMAT CSV\n1,first\n"
                    }}

    send(server, {:respond, 1, "first"})
    assert Task.await(first) == {:ok, 200, [{"x-test", "yes"}, {"content-length", "5"}], "first"}

    second = query_async(pool, "second", settings: [batch: 2])

    assert_receive {:request, ^server, 1, %{target: "/?batch=2", body: "second"}}

    refute_receive {:accepted, ^server, 2}, 50
    send(server, {:respond, 1, "second"})
    assert {:ok, 200, _headers, "second"} = Task.await(second)
  end

  test "a closed connection is removed and the next request opens a fresh one" do
    test_process = self()

    handler = fn connection_id, request ->
      send(test_process, {:request, self(), connection_id, request})

      receive do
        {:close, ^connection_id} -> :close
        {:respond, ^connection_id, body} -> {:respond, 200, [], body}
      end
    end

    {server, url} = start_server(handler)
    pool = start_supervised!({Xh, url: url, max_conns: 1, worker_idle_timeout: :infinity})

    failed = query_async(pool, "closed")
    assert_receive {:request, ^server, 1, %{target: "/", body: "closed"}}
    send(server, {:close, 1})
    assert {:error, %Mint.TransportError{reason: :closed}} = Task.await(failed)

    successful = query_async(pool, "fresh")
    assert_receive {:accepted, ^server, 2}
    assert_receive {:request, ^server, 2, %{target: "/", body: "fresh"}}
    send(server, {:respond, 2, "fresh"})
    assert {:ok, 200, _headers, "fresh"} = Task.await(successful)
  end

  test "a timed-out connection is removed and the next request opens a fresh one" do
    test_process = self()

    handler = fn connection_id, request ->
      send(test_process, {:request, self(), connection_id, request})

      receive do
        {:await_close, ^connection_id} -> :await_close
        {:respond, ^connection_id, body} -> {:respond, 200, [], body}
      end
    end

    {server, url} = start_server(handler)
    pool = start_supervised!({Xh, url: url, max_conns: 1, worker_idle_timeout: :infinity})

    failed = query_async(pool, "timeout", timeout: 40)
    assert_receive {:request, ^server, 1, %{target: "/", body: "timeout"}}
    assert {:error, %Mint.TransportError{reason: :timeout}} = Task.await(failed)
    send(server, {:await_close, 1})
    assert_receive {:connection_closed, ^server, 1}

    successful = query_async(pool, "fresh")
    assert_receive {:accepted, ^server, 2}
    assert_receive {:request, ^server, 2, %{target: "/", body: "fresh"}}
    send(server, {:respond, 2, "fresh"})
    assert {:ok, 200, _headers, "fresh"} = Task.await(successful)
  end

  test "pool checkout timeout exits without sending a query" do
    test_process = self()

    handler = fn connection_id, request ->
      send(test_process, {:request, self(), connection_id, request})

      receive do
        {:respond, ^connection_id, body} -> {:respond, 200, [], body}
      end
    end

    {server, url} = start_server(handler)
    pool = start_supervised!({Xh, url: url, max_conns: 1, worker_idle_timeout: :infinity})

    checked_out = query_async(pool, "held")
    assert_receive {:request, ^server, 1, %{target: "/", body: "held"}}

    queued = Task.async(fn -> catch_exit(Xh.query(pool, "queued", %{}, timeout: 20)) end)

    assert {:timeout, {NimblePool, checkout, _arguments}} = Task.await(queued)
    assert checkout in [:checkout, :checkout!]

    send(server, {:respond, 1, "done"})
    assert {:ok, 200, _headers, "done"} = Task.await(checked_out)
    refute_receive {:request, ^server, _connection_id, %{body: "queued"}}, 50
  end

  test "stopping the pool closes an idle worker connection" do
    test_process = self()

    handler = fn connection_id, request ->
      send(test_process, {:request, self(), connection_id, request})
      {:respond, 204, [], ""}
    end

    {server, url} = start_server(handler)
    {:ok, pool} = Xh.start_link(url: url, max_conns: 1, worker_idle_timeout: :infinity)

    assert {:ok, 204, _headers, ""} =
             Xh.query(pool, "payload", %{}, timeout: @query_timeout)

    assert_receive {:request, ^server, 1, %{target: "/", body: "payload"}}
    assert :ok = Xh.stop(pool)
    assert_receive {:connection_closed, ^server, 1}
  end

  test "start options are validated" do
    assert {:ok, "http://localhost:8123/"} = Xh.validate_url("http://localhost:8123/")
    assert_raise NimbleOptions.ValidationError, fn -> Xh.start_link(max_conns: 0) end
    assert_raise NimbleOptions.ValidationError, fn -> Xh.start_link(transport_opts: :invalid) end
    assert_raise NimbleOptions.ValidationError, fn -> Xh.start_link(url: "ftp://localhost") end

    assert_raise NimbleOptions.ValidationError, fn ->
      Xh.start_link(url: "http://localhost:8123/clickhouse")
    end

    assert_raise NimbleOptions.ValidationError, fn ->
      Xh.start_link(url: "http://user:password@localhost:8123")
    end

    assert_raise NimbleOptions.ValidationError, fn -> Xh.start_link(name: "invalid") end
    assert_raise NimbleOptions.ValidationError, fn -> Xh.start_link(unknown: true) end
  end

  test "a pool can be supervised and addressed by name" do
    test_process = self()

    handler = fn connection_id, request ->
      send(test_process, {:request, self(), connection_id, request})
      {:respond, 200, [], "named"}
    end

    {server, url} = start_server(handler)
    name = XhTest.NamedPool
    child_spec = Xh.child_spec(name: name, url: url, worker_idle_timeout: :infinity)

    assert child_spec.id == name
    pool = start_supervised!(child_spec)
    assert Process.whereis(name) == pool
    refute_receive {:accepted, ^server, _connection_id}, 20

    assert {:ok, 200, _headers, "named"} = Xh.query(name, "")
  end

  defp query_async(pool, statement, options \\ []) do
    options = Keyword.put_new(options, :timeout, @query_timeout)

    Task.async(fn ->
      Xh.query(pool, statement, %{}, options)
    end)
  end

  defp clickhouse_client_port(pool) do
    assert {:ok, 200, _headers, body} =
             Xh.query(pool, @client_port_query, %{}, timeout: @query_timeout)

    String.trim(body)
  end

  defp start_server(handler) do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, packet: :raw, active: false, reuseaddr: true])

    {:ok, {_address, port}} = :inet.sockname(listener)
    test_process = self()
    server = spawn_link(fn -> accept_loop(listener, test_process, handler, 1) end)

    on_exit(fn ->
      :gen_tcp.close(listener)

      if Process.alive?(server) do
        Process.exit(server, :shutdown)
      end
    end)

    {server, "http://localhost:#{port}"}
  end

  defp accept_loop(listener, test_process, handler, connection_id) do
    case :gen_tcp.accept(listener) do
      {:ok, socket} ->
        send(test_process, {:accepted, self(), connection_id})
        connection_loop(socket, test_process, handler, connection_id, "")
        accept_loop(listener, test_process, handler, connection_id + 1)

      {:error, :closed} ->
        :ok
    end
  end

  defp connection_loop(socket, test_process, handler, connection_id, buffer) do
    case read_request(socket, buffer) do
      {:ok, request, rest} ->
        case handler.(connection_id, request) do
          {:respond, status, headers, body} ->
            :ok = send_response(socket, status, headers, body)
            connection_loop(socket, test_process, handler, connection_id, rest)

          {:respond_and_close, status, headers, body} ->
            :ok = send_response(socket, status, headers, body)
            :gen_tcp.close(socket)

          :await_close ->
            assert_socket_closed(socket)
            send(test_process, {:connection_closed, self(), connection_id})

          :close ->
            :gen_tcp.close(socket)
        end

      {:error, :closed} ->
        send(test_process, {:connection_closed, self(), connection_id})
    end
  end

  defp read_request(socket, buffer) do
    with {:ok, head, rest} <- read_head(socket, buffer),
         {:ok, method, target, headers} <- parse_head(head),
         content_length = content_length(headers),
         {:ok, body, rest} <- read_body(socket, rest, content_length) do
      {:ok, %{method: method, target: target, headers: headers, body: body}, rest}
    end
  end

  defp read_head(socket, buffer) do
    case :binary.match(buffer, "\r\n\r\n") do
      {index, 4} ->
        <<head::binary-size(^index), "\r\n\r\n", rest::binary>> = buffer
        {:ok, head, rest}

      :nomatch ->
        case :gen_tcp.recv(socket, 0, :infinity) do
          {:ok, data} -> read_head(socket, buffer <> data)
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp parse_head(head) do
    [request_line | header_lines] = String.split(head, "\r\n")
    [method, target, _version] = String.split(request_line, " ", parts: 3)

    headers =
      Enum.map(header_lines, fn line ->
        [name, value] = String.split(line, ":", parts: 2)
        {String.downcase(name), String.trim_leading(value)}
      end)

    {:ok, method, target, headers}
  end

  defp content_length(headers) do
    case List.keyfind(headers, "content-length", 0) do
      {"content-length", value} -> String.to_integer(value)
      nil -> 0
    end
  end

  defp read_body(_socket, buffer, length) when byte_size(buffer) >= length do
    <<body::binary-size(^length), rest::binary>> = buffer
    {:ok, body, rest}
  end

  defp read_body(socket, buffer, length) do
    missing = length - byte_size(buffer)

    case :gen_tcp.recv(socket, missing, :infinity) do
      {:ok, data} -> read_body(socket, buffer <> data, length)
      {:error, reason} -> {:error, reason}
    end
  end

  defp send_response(socket, status, headers, body) do
    body = IO.iodata_to_binary(body)
    headers = headers ++ [{"content-length", Integer.to_string(byte_size(body))}]

    encoded_headers =
      Enum.map(headers, fn {name, value} -> [name, ": ", value, "\r\n"] end)

    :gen_tcp.send(socket, [
      "HTTP/1.1 ",
      Integer.to_string(status),
      " OK\r\n",
      encoded_headers,
      "\r\n",
      body
    ])
  end

  defp assert_socket_closed(socket) do
    case :gen_tcp.recv(socket, 0, 1_000) do
      {:error, :closed} -> :ok
      other -> exit({:expected_socket_to_close, other})
    end
  end
end
