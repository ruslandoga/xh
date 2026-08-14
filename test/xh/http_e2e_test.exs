defmodule Xh.HTTPE2ETest do
  use ExUnit.Case, async: true

  alias Xh.HTTP

  test "ClickHouse accepts the generated query path" do
    path =
      HTTP.query_path(
        %{
          message: "Привет, 世界 👋",
          count: 9_007_199_254_740_993,
          date: ~D[2026-08-14],
          tags: ["one", "O'Reilly"]
        },
        output_format_json_quote_64bit_integers: false
      )

    query = """
    SELECT
      {message:String} AS message,
      {count:UInt64} AS count,
      {date:Date} AS date,
      {tags:Array(String)} AS tags
    FORMAT JSONEachRow
    """

    assert %{status: 200, body: body} = request(path, query)

    assert body ==
             ~s({"message":"Привет, 世界 👋","count":9007199254740993,"date":"2026-08-14","tags":["one","O'Reilly"]}\n)
  end

  defp request(path, body) do
    {:ok, conn} = Mint.HTTP1.connect(:http, "localhost", 8123, mode: :passive)
    {:ok, conn, ref} = Mint.HTTP1.request(conn, "POST", path, [], body)
    {:ok, conn, response} = receive_response([], conn, ref, %{body: ""})
    {:ok, _conn} = Mint.HTTP1.close(conn)
    response
  end

  defp receive_response([], conn, ref, response) do
    {:ok, conn, entries} = Mint.HTTP1.recv(conn, 0, 5_000)
    receive_response(entries, conn, ref, response)
  end

  defp receive_response([entry | entries], conn, ref, response) do
    case entry do
      {kind, ^ref, value} when kind in [:status, :headers] ->
        receive_response(entries, conn, ref, Map.put(response, kind, value))

      {:data, ^ref, data} ->
        receive_response(entries, conn, ref, Map.update!(response, :body, &(&1 <> data)))

      {:done, ^ref} ->
        {:ok, conn, response}

      {:error, ^ref, error} ->
        {:error, conn, error}
    end
  end
end
