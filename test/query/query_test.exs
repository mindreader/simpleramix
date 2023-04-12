defmodule QueryTest do

  use ExUnit.Case, async: true

  require Simpleramix

  describe "sort/3 with :desc sorts nils last" do 
    query = Simpleramix.timeseries()
            |> Simpleramix.set_granularity(:day)
            |> Simpleramix.datasource("my_datasource")
            |> Simpleramix.put_context(:skipEmptyBuckets, true)
            |> Simpleramix.add_interval("2019-03-01T00:00:00+00:00", "2019-03-04T00:00:00+00:00")
            |> Simpleramix.add_interval(DateTime.utc_now(), DateTime.utc_now())
            |> Simpleramix.add_aggregation(:total, longSum(:__count))
            |> Simpleramix.add_aggregation(:rows, count(:__count))
            |> Simpleramix.add_post_aggregation(:doublesum, aggregations.sum * 2)
            |> Simpleramix.add_virtual_column(:foo, expression("json_value(parse_json(to_json_string(\"foo\")),'$.rhs', 'STRING'))", "STRING"))
            |> Simpleramix.add_filter(dimensions.foo == "bar")
            |> Simpleramix.set_bound(:minTime)
            |> Simpleramix.set_to_include(:all)


    assert query.granularity == :day
    assert Enum.count(query.aggregations) == 2
    assert Enum.count(query.intervals) == 2
  end
end
