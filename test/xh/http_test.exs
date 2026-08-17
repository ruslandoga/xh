defmodule Xh.HTTPTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Xh.HTTP

  setup do
    pool = start_supervised!(Xh)
    {:ok, pool: pool}
  end

  test "builds query paths" do
    assert HTTP.query_path(%{}) == "/"
    assert HTTP.query_path(%{}, readonly: 1) == "/?readonly=1"
  end

  test "encodes temporal edge cases" do
    params = %{
      time: ~T[12:34:56.123],
      epoch: ~U[1970-01-01 00:00:00Z],
      before_epoch: ~U[1969-12-31 23:59:59Z],
      fractional: ~U[1970-01-01 00:00:00.001Z]
    }

    assert HTTP.query_path(params) |> decode_query() == %{
             "param_time" => "12:34:56.123",
             "param_epoch" => "00000",
             "param_before_epoch" => "-00001",
             "param_fractional" => "0.001"
           }
  end

  test "accepts named parameters only" do
    assert_raise FunctionClauseError, fn -> apply(HTTP, :query_path, [[1, 2]]) end
  end

  test "rejects non-finite Decimal parameters" do
    assert_raise ArgumentError, "ClickHouse Decimal values must be finite", fn ->
      HTTP.query_path(%{value: Decimal.new("NaN")})
    end
  end

  property "timeout round trips never extend the timeout" do
    check all(timeout <- integer(0..60_000)) do
      round_tripped = timeout |> HTTP.to_deadline() |> HTTP.to_timeout()
      assert round_tripped <= timeout
      assert round_tripped >= max(timeout - 50, 0)
    end
  end

  property "deadline round trips preserve the absolute deadline" do
    check all(offset <- integer(0..60_000)) do
      deadline = {:deadline, System.monotonic_time(:millisecond) + offset}
      {:deadline, original} = deadline
      {:deadline, round_tripped} = deadline |> HTTP.to_timeout() |> HTTP.to_deadline()
      assert_in_delta round_tripped, original, 50
    end
  end

  test "expired deadlines return zero" do
    assert HTTP.to_timeout({:deadline, System.monotonic_time(:millisecond) - 1}) == 0
  end

  test "ClickHouse accepts the generated query path", %{pool: pool} do
    statement = """
    SELECT
      ({message:String} = 'Привет, 世界 👋') AND
      ({decimal:Decimal(5, 4)} = CAST('1.2300', 'Decimal(5, 4)')) AND
      ({date:Date} = toDate('2026-08-14')) AND
      ({naive:DateTime} = toDateTime('2026-08-14 12:34:56')) AND
      ({epoch:DateTime} = toDateTime(0)) AND
      ({before_epoch:DateTime64(0)} = toDateTime64('1969-12-31 23:59:59', 0)) AND
      ({fractional:DateTime64(3)} = toDateTime64('1970-01-01 00:00:00.001', 3)) AND
      ({tags:Array(String)} = ['one', 'O''Reilly']) AND
      ({tuple:Tuple(UInt8, Bool)} = (1, true)) AND
      ({map:Map(String, String)}['key'] = 'value') AND
      ({readonly:UInt8} = 1) AS ok,
      {count:UInt64} AS count
    FORMAT JSONEachRow
    """

    params = %{
      "message" => "Привет, 世界 👋",
      "count" => 9_007_199_254_740_993,
      "decimal" => Decimal.new("1.2300"),
      "date" => ~D[2026-08-14],
      "naive" => ~N[2026-08-14 12:34:56],
      "epoch" => ~U[1970-01-01 00:00:00Z],
      "before_epoch" => ~U[1969-12-31 23:59:59Z],
      "fractional" => ~U[1970-01-01 00:00:00.001Z],
      "tags" => ["one", "O'Reilly"],
      "tuple" => {1, true},
      "map" => %{"key" => "value"},
      "readonly" => 1
    }

    settings = [
      {"readonly", 1},
      {"output_format_json_quote_64bit_integers", false}
    ]

    assert {:ok, 200, _headers, body} =
             Xh.query(pool, statement, params, settings: settings)

    assert JSON.decode!(body) == %{"ok" => 1, "count" => 9_007_199_254_740_993}
  end

  property "generated named parameters round-trip through ClickHouse", %{pool: pool} do
    check all(
            integer <- integer(-9_223_372_036_854_775_808..9_223_372_036_854_775_807),
            string <- string([?\t, ?\n, ?\\, 32..126, 0x400..0x4FF], max_length: 32),
            max_runs: 25
          ) do
      assert {:ok, 200, _headers, body} =
               Xh.query(
                 pool,
                 "SELECT toString({integer:Int64}) AS integer, hex({string:String}) AS string FORMAT JSONEachRow",
                 %{"integer" => integer, "string" => string}
               )

      assert JSON.decode!(body) == %{
               "integer" => Integer.to_string(integer),
               "string" => Base.encode16(string)
             }
    end
  end

  defp decode_query(target) do
    [_path, query] = String.split(target, "?", parts: 2)
    URI.decode_query(query)
  end
end
