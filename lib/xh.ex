defmodule Xh do
  @moduledoc """
  A small, raw ClickHouse HTTP/1 transport.

  Each pool is bound to the origin configured by `:url`. Connections are opened
  lazily, used by one query at a time, and reused while they remain healthy.

  `query/2` buffers the complete response. It is intended for small insert
  acknowledgements, not large query results, and never retries a query.
  """

  @behaviour NimblePool

  alias Xh.HTTP

  @query_timeout to_timeout(second: 30)

  @start_options_schema [
    name: [
      type: {:custom, __MODULE__, :validate_name, []},
      doc: "The pool name. It may be an atom or a `:via` tuple."
    ],
    url: [
      type: {:custom, __MODULE__, :validate_url, []},
      default: "http://localhost:8123",
      doc: "The HTTP or HTTPS endpoint. Its path prefixes query targets."
    ],
    pool_size: [
      type: :pos_integer,
      default: 20,
      doc: "The maximum number of concurrent HTTP/1 connections."
    ],
    worker_idle_timeout: [
      type: :timeout,
      default: to_timeout(second: 5),
      doc: "How long an idle connection is retained; `:infinity` disables expiration."
    ],
    transport_opts: [
      type: :keyword_list,
      default: [],
      doc: "Transport options passed to `Mint.HTTP1.connect/4`."
    ]
  ]

  @typedoc "Options accepted by `start_link/1`."
  @type start_option :: unquote(NimbleOptions.option_typespec(@start_options_schema))

  @typedoc "A ClickHouse SQL statement, optionally followed by encoded data."
  @type query_statement :: iodata()

  @typedoc "Named ClickHouse query parameters."
  @type query_params :: %{String.t() => term()}

  @typedoc "Options accepted by `query/4`."
  @type query_option ::
          {:headers, Mint.Types.headers()}
          | {:settings, Enumerable.t()}
          | {:timeout, timeout() | HTTP.deadline()}

  @typedoc "A fully buffered raw HTTP response."
  @type response ::
          {:ok, Mint.Types.status(), Mint.Types.headers(), body :: binary()}
          | {:error, Mint.Types.error() | term()}

  @doc false
  def validate_name(name) when is_atom(name), do: {:ok, name}
  def validate_name({:via, module, _term} = via) when is_atom(module), do: {:ok, via}

  def validate_name(name) do
    {:error, "expected an atom or a {:via, module, term} tuple, got: #{inspect(name)}"}
  end

  @doc false
  def validate_url(url) when is_binary(url) do
    with {:ok, uri} <- URI.new(url),
         true <- uri.scheme in ["http", "https"],
         true <- is_binary(uri.host) and uri.host != "",
         true <- is_nil(uri.query),
         true <- is_nil(uri.fragment) do
      {:ok, url}
    else
      _ ->
        {:error,
         "expected an HTTP(S) URL with a host and without a query or fragment, got: #{inspect(url)}"}
    end
  end

  def validate_url(url), do: {:error, "expected a string, got: #{inspect(url)}"}

  @doc """
  Starts a lazy, single-origin HTTP/1 pool.

  Supported options:

  #{NimbleOptions.docs(@start_options_schema)}
  """
  @spec start_link([start_option]) :: GenServer.on_start()
  def start_link(options \\ []) do
    options = NimbleOptions.validate!(options, @start_options_schema)
    endpoint = endpoint(Keyword.fetch!(options, :url), Keyword.fetch!(options, :transport_opts))

    worker_idle_timeout =
      case Keyword.fetch!(options, :worker_idle_timeout) do
        :infinity -> nil
        timeout -> timeout
      end

    NimblePool.start_link(
      worker: {__MODULE__, endpoint},
      pool_size: Keyword.fetch!(options, :pool_size),
      worker_idle_timeout: worker_idle_timeout,
      lazy: true,
      name: Keyword.get(options, :name)
    )
  end

  @doc "Returns a child specification for a pool."
  @spec child_spec([start_option]) :: Supervisor.child_spec()
  def child_spec(options) do
    %{id: Keyword.get(options, :name, __MODULE__), start: {__MODULE__, :start_link, [options]}}
  end

  @doc "Stops a pool."
  @spec stop(NimblePool.pool(), reason :: term(), timeout()) :: :ok
  def stop(pool, reason \\ :normal, timeout \\ :infinity) do
    NimblePool.stop(pool, reason, timeout)
  end

  @doc """
  Executes a ClickHouse query and buffers its complete HTTP response.

  `statement` may be a SQL string or iodata containing SQL followed by encoded
  insert data. `params` are named query parameters. Supported options are:

    * `:headers` - HTTP headers passed to Mint.
    * `:settings` - ClickHouse settings added to the query string.
    * `:timeout` - A relative timeout or absolute monotonic deadline; defaults
      to 30 seconds.

  The same deadline covers pool checkout, connection establishment, query
  transmission, and response receipt.

  The query is never retried. If its connection fails or times out, that
  connection is closed and removed from the pool.
  """
  @spec query(NimblePool.pool(), query_statement(), query_params(), [query_option()]) ::
          response()
  def query(pool, statement, params \\ %{}, options \\ [])
      when (is_binary(statement) or is_list(statement)) and is_map(params) and is_list(options) do
    target = HTTP.query_path(params, Keyword.get(options, :settings, []))
    headers = Keyword.get(options, :headers, [])
    timeout_or_deadline = Keyword.get(options, :timeout, @query_timeout)

    execute(pool, target, headers, statement, timeout_or_deadline)
  end

  defp execute(pool, target, headers, body, timeout_or_deadline) do
    deadline = HTTP.to_deadline(timeout_or_deadline)
    checkout_timeout = HTTP.to_timeout(deadline)

    try do
      NimblePool.checkout!(
        pool,
        :request,
        fn from, conn_or_endpoint ->
          case exchange(from, conn_or_endpoint, "POST", target, headers, body, deadline) do
            {:ok, conn, status, response_headers, response_body} ->
              state =
                if Mint.HTTP1.open?(conn) do
                  {:checkin, conn}
                else
                  {:remove, Mint.TransportError.exception(reason: :closed)}
                end

              {{:ok, status, response_headers, response_body}, state}

            {:error, reason} ->
              {{:error, reason}, {:remove, reason}}
          end
        end,
        checkout_timeout
      )
    catch
      :exit, {:timeout, {NimblePool, checkout, _arguments}}
      when checkout in [:checkout, :checkout!] ->
        {:error, timeout_error()}
    end
  end

  @impl NimblePool
  def init_pool(endpoint), do: {:ok, endpoint}

  @impl NimblePool
  def init_worker(endpoint), do: {:ok, :disconnected, endpoint}

  @impl NimblePool
  def handle_checkout(:request, _from, :disconnected, endpoint) do
    {:ok, {:connect, endpoint}, :disconnected, endpoint}
  end

  def handle_checkout(:request, _from, %Mint.HTTP1{} = conn, endpoint) do
    {:ok, {:connected, conn}, conn, endpoint}
  end

  @impl NimblePool
  def handle_update({:connected, conn}, :disconnected, endpoint) do
    {:ok, conn, endpoint}
  end

  @impl NimblePool
  def handle_checkin({:checkin, conn}, _from, _previous, endpoint) do
    {:ok, conn, endpoint}
  end

  def handle_checkin({:remove, reason}, _from, _previous, endpoint) do
    {:remove, reason, endpoint}
  end

  @impl NimblePool
  def handle_ping(_conn, _endpoint), do: {:remove, :worker_idle_timeout}

  @impl NimblePool
  def terminate_worker(_reason, :disconnected, endpoint), do: {:ok, endpoint}

  def terminate_worker(_reason, conn, endpoint) do
    _ = Mint.HTTP1.close(conn)
    {:ok, endpoint}
  end

  defp endpoint(url, transport_opts) do
    %URI{scheme: scheme, host: host, port: port, path: path} = URI.parse(url)

    %{
      scheme: String.to_existing_atom(scheme),
      host: host,
      port: port,
      path: normalize_base_path(path),
      transport_opts: transport_opts
    }
  end

  defp normalize_base_path(path) when path in [nil, "", "/"], do: ""
  defp normalize_base_path(path), do: String.trim_trailing(path, "/")

  defp exchange(from, conn_or_endpoint, method, target, headers, body, deadline) do
    with {:ok, conn} <- connect(from, conn_or_endpoint, deadline),
         {:ok, conn, ref} <-
           transmit(
             conn,
             method,
             request_target(conn_or_endpoint, target),
             headers,
             body,
             deadline
           ),
         {:ok, conn, status, response_headers, response_body} <-
           receive_response(conn, ref, deadline),
         :ok <- restore_transport_options(conn) do
      {:ok, conn, status, response_headers, response_body}
    else
      {:error, conn, reason} ->
        _ = abort_connection(conn)
        {:error, reason}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp connect(from, {:connect, endpoint}, deadline) do
    timeout = HTTP.to_timeout(deadline)

    transport_opts =
      endpoint.transport_opts
      |> Keyword.put(:timeout, timeout)

    case Mint.HTTP1.connect(endpoint.scheme, endpoint.host, endpoint.port,
           mode: :passive,
           transport_opts: transport_opts
         ) do
      {:ok, conn} ->
        {pool, _ref} = from
        conn = Mint.HTTP1.put_private(conn, :xh_endpoint, endpoint)

        case Mint.HTTP1.controlling_process(conn, pool) do
          {:ok, conn} ->
            :ok = NimblePool.update(from, {:connected, conn})
            {:ok, conn}

          {:error, reason} ->
            _ = Mint.HTTP1.close(conn)
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp connect(_from, {:connected, conn}, _deadline), do: {:ok, conn}

  defp request_target({:connect, endpoint}, target), do: join_target(endpoint.path, target)

  defp request_target({:connected, conn}, target),
    do: join_target(Mint.HTTP1.get_private(conn, :xh_endpoint).path, target)

  defp join_target("", target), do: target
  defp join_target(base_path, "/" <> _ = target), do: base_path <> target
  defp join_target(_base_path, target), do: target

  defp transmit(conn, method, target, headers, body, deadline) do
    case HTTP.to_timeout(deadline) do
      0 ->
        {:error, conn, timeout_error()}

      timeout ->
        with :ok <- configure_send_timeout(conn, timeout) do
          Mint.HTTP1.request(conn, method, target, headers, body)
        else
          {:error, reason} -> {:error, conn, reason}
        end
    end
  end

  defp configure_send_timeout(conn, deadline_timeout) do
    endpoint = Mint.HTTP1.get_private(conn, :xh_endpoint)
    configured_timeout = Keyword.get(endpoint.transport_opts, :send_timeout, :infinity)
    send_timeout = min_timeout(deadline_timeout, configured_timeout)

    send_timeout_close =
      deadline_timeout != :infinity or
        Keyword.get(endpoint.transport_opts, :send_timeout_close, false)

    options = [send_timeout: send_timeout, send_timeout_close: send_timeout_close]

    linger =
      if deadline_timeout == :infinity,
        do: Keyword.get(endpoint.transport_opts, :linger, {false, 0}),
        else: {true, 0}

    options = Keyword.put(options, :linger, linger)
    socket = Mint.HTTP1.get_socket(conn)

    result =
      case endpoint.scheme do
        :http -> :inet.setopts(socket, options)
        :https -> :ssl.setopts(socket, options)
      end

    case result do
      :ok -> :ok
      {:error, reason} -> {:error, Mint.TransportError.exception(reason: reason)}
    end
  end

  defp min_timeout(:infinity, configured), do: configured
  defp min_timeout(deadline, :infinity), do: deadline
  defp min_timeout(deadline, configured), do: min(deadline, configured)

  defp restore_transport_options(conn) do
    if Mint.HTTP1.open?(conn) do
      case configure_send_timeout(conn, :infinity) do
        :ok -> :ok
        {:error, reason} -> {:error, conn, reason}
      end
    else
      :ok
    end
  end

  defp abort_connection(conn) do
    endpoint = Mint.HTTP1.get_private(conn, :xh_endpoint)
    socket = Mint.HTTP1.get_socket(conn)

    _ =
      case endpoint.scheme do
        :http -> :inet.setopts(socket, linger: {true, 0})
        :https -> :ssl.setopts(socket, linger: {true, 0})
      end

    Mint.HTTP1.close(conn)
  end

  defp receive_response(conn, ref, deadline) do
    receive_response(conn, ref, nil, [], [], deadline)
  end

  defp receive_response(conn, ref, status, headers, body, deadline) do
    case Mint.HTTP1.recv(conn, 0, HTTP.to_timeout(deadline)) do
      {:ok, conn, responses} ->
        case reduce_responses(responses, ref, status, headers, body) do
          {:done, status, headers, body} ->
            {:ok, conn, status, headers, body |> Enum.reverse() |> IO.iodata_to_binary()}

          {:more, status, headers, body} ->
            receive_response(conn, ref, status, headers, body, deadline)

          {:error, reason} ->
            {:error, conn, reason}
        end

      {:error, conn, reason, _responses} ->
        {:error, conn, reason}
    end
  end

  defp reduce_responses([{:status, ref, status} | responses], ref, _status, _headers, _body) do
    reduce_responses(responses, ref, status, [], [])
  end

  defp reduce_responses([{:headers, ref, new_headers} | responses], ref, status, headers, body) do
    reduce_responses(responses, ref, status, headers ++ new_headers, body)
  end

  defp reduce_responses([{:data, ref, data} | responses], ref, status, headers, body) do
    reduce_responses(responses, ref, status, headers, [data | body])
  end

  defp reduce_responses([{:done, ref} | _responses], ref, status, headers, body) do
    {:done, status, headers, body}
  end

  defp reduce_responses([{:error, ref, reason} | _responses], ref, _status, _headers, _body) do
    {:error, reason}
  end

  defp reduce_responses([], _ref, status, headers, body) do
    {:more, status, headers, body}
  end

  defp timeout_error, do: Mint.TransportError.exception(reason: :timeout)
end
