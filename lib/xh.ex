defmodule Xh do
  @moduledoc """
  A small, raw ClickHouse HTTP/1 transport.

  Each pool is bound to the origin configured by `:url`. Connections are opened
  lazily, used by one query at a time, and reused while they remain healthy.

  `query/2` buffers the complete response. It is intended for small insert
  acknowledgements, not large query results, and never retries a query.
  """

  @behaviour NimblePool

  @dialyzer :no_improper_lists

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
      doc: "The HTTP or HTTPS origin. Authentication is not yet supported."
    ],
    max_conns: [
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
         true <- is_nil(uri.userinfo),
         true <- uri.path in [nil, "", "/"],
         true <- is_nil(uri.query),
         true <- is_nil(uri.fragment) do
      {:ok, url}
    else
      _ ->
        {:error,
         "expected an HTTP(S) origin with neither userinfo, a non-root path, a query, nor a fragment, got: #{inspect(url)}"}
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
    name = Keyword.get(options, :name)
    url = Keyword.fetch!(options, :url)
    max_conns = Keyword.fetch!(options, :max_conns)
    worker_idle_timeout = Keyword.fetch!(options, :worker_idle_timeout)
    transport_opts = Keyword.fetch!(options, :transport_opts)

    %URI{scheme: scheme, host: host, port: port} = URI.parse(url)

    endpoint = %{
      scheme: String.to_existing_atom(scheme),
      host: host,
      port: port,
      transport_opts: transport_opts
    }

    worker_idle_timeout = if worker_idle_timeout == :infinity, do: nil, else: worker_idle_timeout

    NimblePool.start_link(
      worker: {__MODULE__, endpoint},
      pool_size: max_conns,
      worker_idle_timeout: worker_idle_timeout,
      lazy: true,
      name: name
    )
  end

  @doc "Returns a child specification for a pool. See `start_link/1` for supported options."
  @spec child_spec([start_option]) :: Supervisor.child_spec()
  def child_spec(options) do
    id = Keyword.get(options, :name, __MODULE__)
    %{id: id, start: {__MODULE__, :start_link, [options]}}
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

  The same deadline covers pool checkout, connection establishment, and
  response receipt. Request transmission uses Mint's transport behavior and
  the configured transport options.

  If pool checkout exceeds the deadline, this function exits with NimblePool's
  timeout. The exit is intentionally not converted into an error tuple because
  a late checkout reply could otherwise remain in the caller's mailbox.

  The query is never retried. If its connection fails or times out, that
  connection is closed and removed from the pool.
  """
  @spec query(NimblePool.pool(), query_statement(), query_params(), [query_option()]) ::
          response()
  def query(pool, statement, params \\ %{}, options \\ []) do
    target = HTTP.query_path(params, Keyword.get(options, :settings, []))
    headers = Keyword.get(options, :headers, [])
    timeout_or_deadline = Keyword.get(options, :timeout, @query_timeout)
    deadline = HTTP.to_deadline(timeout_or_deadline)

    NimblePool.checkout!(
      pool,
      :request,
      fn from, conn_or_endpoint ->
        case ensure_connected(from, conn_or_endpoint, deadline) do
          {:ok, conn} ->
            case request(conn, target, headers, statement, deadline) do
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

          {:error, reason} ->
            {{:error, reason}, {:remove, reason}}
        end
      end,
      HTTP.to_timeout(deadline)
    )
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
  @dialyzer {:nowarn_function, terminate_worker: 3}
  @spec terminate_worker(term(), :disconnected | Mint.HTTP1.t(), map()) :: {:ok, map()}
  def terminate_worker(_reason, conn, endpoint) do
    with %Mint.HTTP1{} <- conn, do: Mint.HTTP1.close(conn)
    {:ok, endpoint}
  end

  defp request(conn, target, headers, body, deadline) do
    with {:ok, conn, ref} <- Mint.HTTP1.request(conn, "POST", target, headers, body),
         {:ok, conn, status, response_headers, response_body} <-
           receive_response(conn, ref, deadline) do
      {:ok, conn, status, response_headers, response_body}
    else
      {:error, conn, reason} ->
        _ = Mint.HTTP1.close(conn)
        {:error, reason}
    end
  end

  defp ensure_connected(from, {:connect, endpoint}, deadline) do
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

  defp ensure_connected(_from, {:connected, conn}, _deadline), do: {:ok, conn}

  defp receive_response(conn, ref, deadline) do
    receive_response(conn, ref, nil, [], [], deadline)
  end

  defp receive_response(conn, ref, status, headers, body, deadline) do
    case Mint.HTTP1.recv(conn, 0, HTTP.to_timeout(deadline)) do
      {:ok, conn, responses} ->
        case reduce_responses(responses, ref, status, headers, body) do
          {:done, status, headers, body} ->
            {:ok, conn, status, headers, IO.iodata_to_binary(body)}

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
    reduce_responses(responses, ref, status, headers, [body | data])
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
end
