defmodule Simpleramix.MixProject do
  use Mix.Project

  def project do
    [
      app: :simpleramix,
      version: "1.5.1",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      description: "Client library for sending requests to Druid.",
      source_url: "https://github.com/simplecastapps/simpleramix",
      package: package(),
      deps: deps(),

      # Docs
      name: "Simpleramix",
      source_url: "https://github.com/simplecastapps/simpleramix",
      homepage_url: "https://github.com/simplecastapps/simpleramix",
      docs: [
        # The main page in the docs
        main: "Simpleramix",
        extras: ["README.md"]
      ]
    ]
  end

  defp package do
    [
      files: ["config", "lib", "mix.exs", "README*", "LICENSE*"],
      maintainers: ["Magnus Henoch"],
      licenses: ["Apache-2.0"],
      links: %{github: "https://github.com/simplecastapps/simpleramix"}
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
      {:jason, "~> 1.1"},
      {:httpoison, "~> 1.0"},
      {:timex, "~> 3.1"},
      {:dialyxir, "~> 1.0-rc.3", only: [:dev], runtime: false},
      {:credo, "~> 1.6.5", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.27.3", only: :dev, runtime: false}
    ]
  end
end
