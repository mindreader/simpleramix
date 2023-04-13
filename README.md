# Simpleramix

An open-source client library for sending requests to [Apache Druid][druid] from applications written in Elixir. The project uses [HTTPoison][httpoison] as an HTTP client for sending queries.

This was inspired by Simpleramix, and is in fact almost identical except that its functions are composable, allowing for better reuse, and forked simply because it feels unlikely they would accept the number of changes we've made.

[druid]: http://druid.io/
[httpoison]: https://github.com/edgurgel/httpoison

## Getting Started

Add Simpleramix as a dependency to your project.

```elixir
defp deps do
  [
    {:simpleramix, "~> 1.0.0"}
  ]
end
```

## Configuration 

Simpleramix requires a Druid Broker profile to be defined in the configuration of your application.

```elixir
config :simpleramix,
  request_timeout: 120_000,
  query_priority:  0,
  broker_profiles: [
    default: [
      base_url:       "https://druid-broker-host:9088",
      cacertfile:     "path/to/druid-certificate.crt",
      http_username:  "username",
      http_password:  "password"
    ]
  ]
```

* `request_timeout`: Query timeout in millis to be used in [`Context`](context-druid-doc-link) of all Druid queries. 
* `query_priority`: Priority to be used in [`Context`](context-druid-doc-link) of all Druid queries. 

[context-druid-doc-link]: http://druid.io/docs/latest/querying/query-context.html

The `cacertfile` option in the broker profile names a file that contains the CA certificate for the Druid broker. Alternatively you can specify the certificate as a string in PEM format (starting with `-----BEGIN CERTIFICATE-----`) in the `cacert` option.

## Usage

TODO

## License
Except as otherwise noted this software is licensed under the [Apache License, Version 2.0]((http://www.apache.org/licenses/LICENSE-2.0))

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the 
License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an 
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the 
specific language governing permissions and limitations under the License.

The code was Copyright 2018-2019 Game Analytics Limited and/or its affiliates. 
