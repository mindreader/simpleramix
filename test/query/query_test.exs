defmodule QueryTest do
  use ExUnit.Case, async: true

  describe "sort/3 with :desc sorts nils last" do
    use Simpleramix

    query =
      timeseries("my_datasource",
        granularity: :month,
        intervals: [
          {Timex.now(), Timex.now()}
        ],
        aggregations: [
          total_two: longSum(:__count)
        ],
        post_aggregations: [
          triplesum: aggregations.sum * 3
        ],
        virtual_columns: [
          foobar: expression("count(\"triplesum\")", :long)
        ],
        context: %{
          skipEmptyBuckets: false
        },
        filter: dimensions.foobar == "baz",
        dimensions: [:foo, :bar],
        subtotals_spec: [[:d1], [:d2, :d3]]
      )

    assert query.query_type == :timeseries
    assert query.granularity == :month
    assert Enum.count(query.aggregations) == 1
    assert Enum.count(query.post_aggregations) == 1
    assert Enum.count(query.virtual_columns) == 1
    assert Enum.count(query.intervals) == 1
    assert query.context.skipEmptyBuckets == false
    assert query.filter.dimension == :foobar

    assert query.dimensions == [:foo, :bar]

    assert query.subtotals_spec == [[:d1], [:d2, :d3]]

    query =
      query
      |> Simpleramix.set_granularity(:day)
      |> Simpleramix.put_context(:skipEmptyBuckets, true)
      |> Simpleramix.add_interval("2019-03-01T00:00:00+00:00", "2019-03-04T00:00:00+00:00")
      |> Simpleramix.add_interval(DateTime.utc_now(), DateTime.utc_now())
      |> Simpleramix.add_aggregation(:total, longSum(:__count))
      |> Simpleramix.add_aggregation(:rows, count(:__count))
      |> Simpleramix.add_post_aggregation(:doublesum, aggregations.sum * 2)
      |> Simpleramix.add_virtual_column(
        :foo,
        expression("json_value(parse_json(to_json_string(\"foo\")),'$.rhs', 'STRING'))", :string)
      )
      |> Simpleramix.set_bound(:minTime)
      |> Simpleramix.set_to_include(:all)
      |> Simpleramix.set_subtotals_spec([[:a1],[:a2]])

    assert Enum.count(query.aggregations) == 3
    assert Enum.count(query.post_aggregations) == 2
    assert Enum.count(query.virtual_columns) == 2
    assert Enum.count(query.intervals) == 3
    assert query.context.skipEmptyBuckets == true
    assert query.subtotals_spec == [[:a1],[:a2]]

    query = query |> Simpleramix.add_filter(dimensions.foo == "bar")

    assert query.filter.type == :and
    assert query.filter.fields |> Enum.count() == 2
  end
end
