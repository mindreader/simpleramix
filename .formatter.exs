locals_without_parens = [ from: 2, timeseries: 2, groupBy: 2, topN: 2 ]

[
  inputs: ["mix.exs", "{config,lib,test}/**/*.{ex,exs}"],
  locals_without_parens: locals_without_parens,
  export: [     locals_without_parens: locals_without_parens   ]
]
