import Config

config :simpleramix,
  request_timeout: 120_000,
  query_priority: 0,
  broker_profiles: [
    default: [
      base_url: "https://druid-broker-host:9088",
      cacertfile: "path/to/druid-certificate.crt",
      http_username: "username",
      http_password: "password"
    ]
  ]
