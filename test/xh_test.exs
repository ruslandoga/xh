defmodule XhTest do
  use ExUnit.Case, async: true

  @request_timeout 1_000
  @clickhouse_url "http://localhost:8123"
  @client_port_query "SELECT port FROM system.processes WHERE query_id=currentQueryID() FORMAT TabSeparated"

  test "real ClickHouse requests reuse the same HTTP connection" do
    pool =
      start_supervised!(
        {Xh, url: @clickhouse_url, pool_size: 1, worker_idle_timeout: :infinity},
        id: make_ref()
      )

    first_port = clickhouse_client_port(pool)
    second_port = clickhouse_client_port(pool)

    assert second_port == first_port
  end

  test "a connection closed by real ClickHouse is replaced" do
    pool =
      start_supervised!(
        {Xh, url: @clickhouse_url, pool_size: 1, worker_idle_timeout: :infinity},
        id: make_ref()
      )

    first_port = clickhouse_client_port(pool)

    assert {:ok, 200, headers, body} =
             Xh.request(
               pool,
               {"POST", "/", [{"connection", "close"}], @client_port_query},
               @request_timeout
             )

    assert String.trim(body) == first_port
    assert {_, connection} = List.keyfind(headers, "connection", 0)
    assert String.downcase(connection) == "close"
    refute clickhouse_client_port(pool) == first_port
  end

  test "a timed-out real ClickHouse request is removed before the next request" do
    pool =
      start_supervised!(
        {Xh, url: @clickhouse_url, pool_size: 1, worker_idle_timeout: :infinity},
        id: make_ref()
      )

    first_port = clickhouse_client_port(pool)

    assert {:error, %Mint.TransportError{reason: :timeout}} =
             Xh.request(pool, {"POST", "/", [], "SELECT sleep(0.2)"}, 20)

    refute clickhouse_client_port(pool) == first_port
  end

  test "the pool is lazy and a second request reuses its connection" do
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
         url: url <> "/clickhouse",
         pool_size: 1,
         worker_idle_timeout: :infinity,
         transport_opts: [nodelay: true]}
      )

    refute_receive {:accepted, ^server, _connection_id}, 50

    first = request_async(pool, "/insert?batch=1")

    assert_receive {:accepted, ^server, 1}
    assert_receive {:request, ^server, 1, %{target: "/clickhouse/insert?batch=1"}}
    send(server, {:respond, 1, "first"})
    assert Task.await(first) == {:ok, 200, [{"x-test", "yes"}, {"content-length", "5"}], "first"}

    second = request_async(pool, "/insert?batch=2")

    assert_receive {:request, ^server, 1, %{target: "/clickhouse/insert?batch=2"}}
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
    pool = start_supervised!({Xh, url: url, pool_size: 1, worker_idle_timeout: :infinity})

    failed = request_async(pool, "/closed")
    assert_receive {:request, ^server, 1, %{target: "/closed"}}
    send(server, {:close, 1})
    assert {:error, %Mint.TransportError{reason: :closed}} = Task.await(failed)

    successful = request_async(pool, "/fresh")
    assert_receive {:accepted, ^server, 2}
    assert_receive {:request, ^server, 2, %{target: "/fresh"}}
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
    pool = start_supervised!({Xh, url: url, pool_size: 1, worker_idle_timeout: :infinity})

    failed = request_async(pool, "/timeout", 40)
    assert_receive {:request, ^server, 1, %{target: "/timeout"}}
    assert {:error, %Mint.TransportError{reason: :timeout}} = Task.await(failed)
    send(server, {:await_close, 1})
    assert_receive {:connection_closed, ^server, 1}

    successful = request_async(pool, "/fresh")
    assert_receive {:accepted, ^server, 2}
    assert_receive {:request, ^server, 2, %{target: "/fresh"}}
    send(server, {:respond, 2, "fresh"})
    assert {:ok, 200, _headers, "fresh"} = Task.await(successful)
  end

  test "pool checkout timeout is returned as a transport error" do
    test_process = self()

    handler = fn connection_id, request ->
      send(test_process, {:request, self(), connection_id, request})

      receive do
        {:respond, ^connection_id, body} -> {:respond, 200, [], body}
      end
    end

    {server, url} = start_server(handler)
    pool = start_supervised!({Xh, url: url, pool_size: 1, worker_idle_timeout: :infinity})

    checked_out = request_async(pool, "/held")
    assert_receive {:request, ^server, 1, %{target: "/held"}}

    assert {:error, %Mint.TransportError{reason: :timeout}} =
             Xh.request(pool, {"POST", "/queued", [], "payload"}, 20)

    send(server, {:respond, 1, "done"})
    assert {:ok, 200, _headers, "done"} = Task.await(checked_out)
    refute_receive {:request, ^server, _connection_id, %{target: "/queued"}}, 50
  end

  test "the deadline bounds request transmission" do
    {server, url} = start_stalled_server()

    pool =
      start_supervised!(
        {Xh,
         url: url, pool_size: 1, worker_idle_timeout: :infinity, transport_opts: [sndbuf: 1_024]}
      )

    body = :binary.copy(<<0>>, 4 * 1_024 * 1_024)
    started_at = System.monotonic_time(:millisecond)

    assert {:error, %Mint.TransportError{reason: :timeout}} =
             Xh.request(pool, {"POST", "/stalled", [], body}, 40)

    assert_receive {:accepted, ^server}
    assert System.monotonic_time(:millisecond) - started_at < 500
  end

  test "stopping the pool closes an idle worker connection" do
    test_process = self()

    handler = fn connection_id, request ->
      send(test_process, {:request, self(), connection_id, request})
      {:respond, 204, [], ""}
    end

    {server, url} = start_server(handler)
    {:ok, pool} = Xh.start_link(url: url, pool_size: 1, worker_idle_timeout: :infinity)

    assert {:ok, 204, _headers, ""} =
             Xh.request(pool, {"POST", "/ack", [], "payload"}, @request_timeout)

    assert_receive {:request, ^server, 1, %{target: "/ack", body: "payload"}}
    assert :ok = Xh.stop(pool)
    assert_receive {:connection_closed, ^server, 1}
  end

  test "start options are validated" do
    assert_raise NimbleOptions.ValidationError, fn -> Xh.start_link(pool_size: 0) end
    assert_raise NimbleOptions.ValidationError, fn -> Xh.start_link(transport_opts: :invalid) end
    assert_raise NimbleOptions.ValidationError, fn -> Xh.start_link(url: "ftp://localhost") end
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

    assert {:ok, 200, _headers, "named"} =
             Xh.request(name, {"POST", "/named", [], ""}, @request_timeout)
  end

  defp request_async(pool, target, timeout \\ @request_timeout) do
    Task.async(fn -> Xh.request(pool, {"POST", target, [], "payload"}, timeout) end)
  end

  defp clickhouse_client_port(pool) do
    assert {:ok, 200, _headers, body} =
             Xh.request(pool, {"POST", "/", [], @client_port_query}, @request_timeout)

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

  defp start_stalled_server do
    {:ok, listener} =
      :gen_tcp.listen(0, [
        :binary,
        packet: :raw,
        active: false,
        reuseaddr: true,
        recbuf: 1_024
      ])

    {:ok, {_address, port}} = :inet.sockname(listener)
    test_process = self()

    server =
      spawn_link(fn ->
        {:ok, socket} = :gen_tcp.accept(listener)
        send(test_process, {:accepted, self()})

        receive do
          :close -> :gen_tcp.close(socket)
        end
      end)

    on_exit(fn ->
      :gen_tcp.close(listener)

      if Process.alive?(server) do
        send(server, :close)
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
