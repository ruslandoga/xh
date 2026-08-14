defmodule Xh.HTTP do
  @moduledoc false

  import Kernel, except: [to_timeout: 1]

  @type deadline :: {:deadline, integer} | :infinity

  @spec query_path(map, Enumerable.t()) :: String.t()
  def query_path(params, settings \\ []) when is_map(params) do
    params = Enum.map(params, fn {key, value} -> {"param_#{key}", encode_param(value)} end)
    settings = Enum.map(settings, fn {key, value} -> {to_string(key), encode_setting(value)} end)

    case URI.encode_query(params ++ settings) do
      "" -> "/"
      encoded -> "/?" <> encoded
    end
  end

  @spec to_deadline(timeout | deadline) :: deadline
  def to_deadline(:infinity), do: :infinity
  def to_deadline({:deadline, timestamp}) when is_integer(timestamp), do: {:deadline, timestamp}

  def to_deadline(timeout) when is_integer(timeout) and timeout >= 0 do
    {:deadline, System.monotonic_time(:millisecond) + timeout}
  end

  @spec to_timeout(timeout | deadline) :: timeout
  def to_timeout(:infinity), do: :infinity
  def to_timeout(timeout) when is_integer(timeout) and timeout >= 0, do: timeout

  def to_timeout({:deadline, timestamp}) when is_integer(timestamp) do
    max(0, timestamp - System.monotonic_time(:millisecond))
  end

  defp encode_param(value) when is_integer(value), do: Integer.to_string(value)
  defp encode_param(value) when is_float(value), do: Float.to_string(value)

  defp encode_param(value) when is_binary(value) do
    escape_param([{"\\", "\\\\"}, {"\t", "\\\t"}, {"\n", "\\\n"}], value)
  end

  defp encode_param(value) when is_boolean(value), do: Atom.to_string(value)
  defp encode_param(nil), do: "\\N"
  defp encode_param(%Decimal{} = decimal), do: decimal_to_string!(decimal)
  defp encode_param(%Date{} = date), do: Date.to_iso8601(date)
  defp encode_param(%NaiveDateTime{} = naive), do: NaiveDateTime.to_iso8601(naive)
  defp encode_param(%Time{} = time), do: Time.to_iso8601(time)

  defp encode_param(%DateTime{microsecond: {_value, precision}} = datetime) when precision > 0 do
    unix = DateTime.to_unix(datetime, Integer.pow(10, precision))
    sign = if unix < 0, do: -1, else: 1

    sign
    |> Decimal.new(abs(unix), -precision)
    |> Decimal.to_string(:normal)
  end

  defp encode_param(%DateTime{} = datetime) do
    unix = DateTime.to_unix(datetime, :second)
    unsigned = unix |> abs() |> Integer.to_string() |> String.pad_leading(5, "0")
    if unix < 0, do: "-" <> unsigned, else: unsigned
  end

  defp encode_param(tuple) when is_tuple(tuple) do
    IO.iodata_to_binary([?(, encode_array_params(Tuple.to_list(tuple)), ?)])
  end

  defp encode_param(array) when is_list(array) do
    IO.iodata_to_binary([?[, encode_array_params(array), ?]])
  end

  defp encode_param(map) when is_map(map) do
    IO.iodata_to_binary([?{, encode_map_params(Map.to_list(map)), ?}])
  end

  defp encode_array_params([last]), do: encode_array_param(last)

  defp encode_array_params([value | rest]) do
    [encode_array_param(value), ?, | encode_array_params(rest)]
  end

  defp encode_array_params([] = empty), do: empty

  defp encode_map_params([last]), do: encode_map_param(last)

  defp encode_map_params([pair | rest]) do
    [encode_map_param(pair), ?, | encode_map_params(rest)]
  end

  defp encode_map_params([] = empty), do: empty

  defp encode_array_param(value) when is_binary(value) do
    [?', escape_param([{"'", "''"}, {"\\", "\\\\"}], value), ?']
  end

  defp encode_array_param(nil), do: "null"

  defp encode_array_param(%module{} = value) when module in [Date, NaiveDateTime, DateTime] do
    [?', encode_param(value), ?']
  end

  defp encode_array_param(value), do: encode_param(value)

  defp encode_map_param({key, value}) do
    [encode_array_param(key), ?:, encode_array_param(value)]
  end

  defp escape_param([{pattern, replacement} | escapes], param) do
    param = String.replace(param, pattern, replacement)
    escape_param(escapes, param)
  end

  defp escape_param([], param), do: param

  @compile inline: [decimal_to_string!: 1]
  defp decimal_to_string!(%Decimal{coef: coefficient}) when coefficient in [:NaN, :inf] do
    raise ArgumentError, "ClickHouse Decimal values must be finite"
  end

  defp decimal_to_string!(decimal), do: Decimal.to_string(decimal, :scientific)

  defp encode_setting(value) when is_binary(value), do: value
  defp encode_setting(value) when is_boolean(value), do: Atom.to_string(value)
  defp encode_setting(value) when is_atom(value), do: Atom.to_string(value)
  defp encode_setting(value) when is_number(value), do: to_string(value)
end
