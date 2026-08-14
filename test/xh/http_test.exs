defmodule Xh.HTTPTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Xh.HTTP

  test "builds query paths" do
    assert HTTP.query_path(%{}) == "/"
    assert HTTP.query_path(%{}, readonly: 1) == "/?readonly=1"
  end

  test "keeps settings distinct from named SQL parameters" do
    target =
      HTTP.query_path(
        %{async_insert: 7},
        async_insert: 1,
        wait_for_async_insert: true
      )

    assert decode_query(target) == %{
             "param_async_insert" => "7",
             "async_insert" => "1",
             "wait_for_async_insert" => "true"
           }
  end

  test "encodes current Ch scalar parameter values" do
    params = %{
      decimal: Decimal.new("1.2300"),
      date: ~D[2026-08-14],
      naive: ~N[2026-08-14 12:34:56],
      time: ~T[12:34:56.123],
      epoch: ~U[1970-01-01 00:00:00Z],
      before_epoch: ~U[1969-12-31 23:59:59Z],
      fractional: ~U[1970-01-01 00:00:00.001Z],
      unicode: "Привет, 世界 👋"
    }

    assert HTTP.query_path(params) |> decode_query() == %{
             "param_decimal" => "1.2300",
             "param_date" => "2026-08-14",
             "param_naive" => "2026-08-14T12:34:56",
             "param_time" => "12:34:56.123",
             "param_epoch" => "00000",
             "param_before_epoch" => "-00001",
             "param_fractional" => "0.001",
             "param_unicode" => "Привет, 世界 👋"
           }
  end

  test "encodes collection parameter values" do
    target =
      HTTP.query_path(%{
        array: ["O'Reilly", nil, ~D[2026-08-14]],
        tuple: {1, true},
        map: %{"key" => "value"}
      })

    assert decode_query(target) == %{
             "param_array" => "['O''Reilly',null,'2026-08-14']",
             "param_tuple" => "(1,true)",
             "param_map" => "{'key':'value'}"
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

  defp decode_query(target) do
    [_path, query] = String.split(target, "?", parts: 2)
    URI.decode_query(query)
  end
end
