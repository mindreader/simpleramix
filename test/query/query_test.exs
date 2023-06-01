defmodule QueryTest do
  use ExUnit.Case, async: true

  describe "sort/3 with :desc sorts nils last" do
    use Simpleramix

    filtervar = "foobar"

    query =
      from("my_datasource",
        query_type: :timeseries,
        granularity: :month,
        intervals: [
          {Timex.now(), Timex.now()}
        ],
        aggregations: [
          total_two: longSum(:__count) when dimensions.foo == "123"
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
        filter: dimensions.foobar == "baz" and dimensions.baz == filtervar,
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
    assert query.filter.type == :and
    assert query.filter.fields |> hd |> Map.get(:dimension) == :foobar
    assert query.filter.fields |> hd |> Map.get(:value) == "baz"
    assert query.filter.fields |> tl |> hd |> Map.get(:dimension) == :baz
    assert query.filter.fields |> tl |> hd |> Map.get(:value) == "foobar"

    assert query.dimensions == [:foo, :bar]

    assert query.subtotals_spec == [[:d1], [:d2, :d3]]

    field_name = "field_name's actual name"
    field_value = "field_value's actual value"
    ob_value = %{key: "ob_value's actual value"}

    time_zone = "America/New_York"

    ob = %{foo: :obs_value}

    foobar = fn x -> x * 2 end

    query =
      query
      |> Simpleramix.set_granularity(:day)
      |> Simpleramix.put_context(:skipEmptyBuckets, true)
      |> Simpleramix.add_interval("2019-03-01T00:00:00+00:00", "2019-03-04T00:00:00+00:00")
      |> Simpleramix.add_interval(DateTime.utc_now(), DateTime.utc_now())
      |> Simpleramix.add_aggregation(
        :field_total,
        longSum(^field_name) when dimensions.foo == field_value
      )
      |> Simpleramix.add_aggregation(
        :field_total_2,
        longSum(:__count) when dimensions.foo == ob_value.key
      )
      |> Simpleramix.add_aggregation(
        :field_total_3,
        longSum(:__count) when dimensions.foo == foobar.(2)
      )

      # TODO would be nice to suport eg. this
      |> Simpleramix.add_aggregation(
        :field_total,
        longSum(ob.foo) when dimensions.foo == ob.foo
      )
      |> Simpleramix.add_aggregation(:total, longSum(:__count))
      |> Simpleramix.add_aggregation(:rows, count(:__count))
      |> Simpleramix.add_aggregations(
        rows2: count(:__count) when dimensions.foo == "row2val",
        rows3: count(:__count)
      )
      |> Simpleramix.add_post_aggregation(:doublesum, aggregations.sum * 2)
      |> Simpleramix.add_post_aggregation(
        :post1,
        hllSketchUnion([aggregations.foo1_without_name, aggregations.bar1_without_name],
          option1: :bar
        )
      )
      |> Simpleramix.add_post_aggregation(
        :post2,
        hllSketchUnion([aggregations["foo2_without_name"], aggregations["bar2_without_name"]],
          option2: :bar
        )
      )
      |> Simpleramix.add_post_aggregation(
        :post3,
        hllSketchUnion([aggregations[:foo3_without_name], aggregations[:bar3_without_name]],
          option3: :bar
        )
      )
      |> Simpleramix.add_post_aggregation(
        :post4,
        hllSketchUnion([foo4: aggregations.foo4_with_name, bar4: aggregations.bar4_with_name],
          option4: :bar
        )
      )
      |> Simpleramix.add_post_aggregation(
        :post4,
        hllSketchUnion([foo5: aggregations[:foo5_with_name], bar5: aggregations[:bar5_with_name]],
          option5: :bar
        )
      )
      |> Simpleramix.add_post_aggregation(
        :post4,
        thetaSketchIntersect(
          foo6: aggregations.foo6_with_name,
          bar6: field_name,
          bar6: ob.foo
        )
      )
      |> Simpleramix.add_post_aggregation(
        :post_inter,
        thetaSketchUnion(
          [
            p0: aggregations.a3,
            p1: aggregations.a4
          ],
          size: 16384
        )
      )
      |> Simpleramix.add_post_aggregations(
        triplesum: aggregations.sum * 3,
        quadsum: aggregations.sum * 4
      )
      |> Simpleramix.add_virtual_column(
        :foo,
        expression("json_value(parse_json(to_json_string(\"foo\")),'$.rhs', 'STRING'))", :string)
      )
      |> Simpleramix.add_virtual_columns(
        foo2:
          expression(
            "json_value(parse_json(to_json_string(\"foo\")),'$.rhs', 'STRING'))",
            :string
          ),
        foo3:
          expression(
            "json_value(parse_json(to_json_string(\"foo\")),'$.rhs', 'STRING'))",
            :string
          )
      )
      |> Simpleramix.add_virtual_columns(
        dow_mon: expression("timestamp_extract(__time,'DOW','#{time_zone}')", :long),
        dow: expression("case_simple(dow_mon == 7,0,dow_mon)", :long),
        hour: expression("timestamp_extract(__time,'HOUR','#{time_zone}')", :long),
        hour_of_week: expression("dow * 24 + hour", :long)
      )
      |> Simpleramix.set_bound(:minTime)
      |> Simpleramix.set_to_include(:all)
      |> Simpleramix.set_subtotals_spec([[:a1], [:a2]])

    assert Enum.count(query.aggregations) == 9
    assert Enum.count(query.post_aggregations) == 11
    assert Enum.count(query.virtual_columns) == 8
    assert Enum.count(query.intervals) == 3
    assert query.context.skipEmptyBuckets == true
    assert query.subtotals_spec == [[:a1], [:a2]]

    geotest = 1

    some_ids = [1, 2, 3]

    mindate = DateTime.utc_now()
    maxdate = DateTime.utc_now()

    dt = Timex.now()

    query =
      query
      |> Simpleramix.add_filter(dimensions.foo == "bar")
      |> Simpleramix.add_filter(dimensions.foo in some_ids)
      |> Simpleramix.add_filter(dimensions.foo not in some_ids)
      |> Simpleramix.add_filter(dimensions.nullfield != nil)
      |> Simpleramix.add_filter(dimensions.nullfield == nil)
      |> Simpleramix.add_filter(dimensions.nullfield != null)
      |> Simpleramix.add_filter(dimensions.sometimesnullfield == null)

      # you should strongly prefer setting intervals to setting these, when
      # involving your time column, because I don't think druid is smart
      # enough to notice it can use this to avoid scanning all segments.
      #
      # That said, it is useful for querying longs and very niche time filtering.
      |> Simpleramix.add_filter(mindate <= dimensions.__time < maxdate)
      |> Simpleramix.add_filter(1 <= dimensions.__time < 2)
      |> Simpleramix.add_filter(dimensions.__time > dt)
      |> Simpleramix.add_filter(dimensions.strfield >= "B")
      |> Simpleramix.add_filter(geotest <= dimensions.foobar < 2)

    assert query.filter.type == :and
    assert query.filter.fields |> Enum.count() == 14
  end
end
