defmodule Xh.MixProject do
  use Mix.Project

  @source_url "https://github.com/ruslandoga/xh"
  @version "0.1.0"

  def project do
    [
      app: :xh,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "ClickHouse HTTP client for Elixir",
      package: package(),
      source_url: @source_url,
      dialyzer: [plt_local_path: "plts", plt_core_path: "plts"]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:decimal, "~> 3.0"},
      {:mint, "~> 1.8"},
      {:nimble_pool, "~> 1.1"},
      {:nimble_options, "~> 1.1"},
      {:dialyxir, "~> 1.0", only: [:dev, :test], runtime: false},
      {:stream_data, "~> 1.1", only: :test}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end
end
