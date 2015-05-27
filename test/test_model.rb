require 'cassandra/uuid'
require 'corm'
require 'logger'
require 'test/unit'


class TestModel < Test::Unit::TestCase

  class FakeModel < Corm::Model

    keyspace  :corm_test
    table     :corm_model_test
    # Not working yet in ruby-driver!
    # properties(
    #   'bloom_filter_fp_chance = 0.01',
    #   'caching = \'{"keys":"ALL", "rows_per_partition":"NONE"}\'',
    #   'comment = ""',
    #   "compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}",
    #   "compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}",
    #   'dclocal_read_repair_chance = 0.1',
    #   'default_time_to_live = 0',
    #   'gc_grace_seconds = 864000',
    #   'max_index_interval = 2048',
    #   'memtable_flush_period_in_ms = 0',
    #   'min_index_interval = 128',
    #   'read_repair_chance = 0.0',
    #   'speculative_retry = "99.0PERCENTILE"'
    # )

    field :uuid_field,      :text,      true
    field :text_field,      :text
    field :int_field,       :int
    field :double_field,    :double
    field :boolean_field,   :boolean
    field :timestamp_field, :timestamp
    field :list_field,      'list<JSON>'
    field :set_field,       'set<JSON>'
    field :set_text_field,  'set<TEXT>'
    field :map_field,       'map<JSON, JSON>'
    field :map_text_field,  'map<TEXT, TEXT>'

  end

  def setup
    @logger = Logger.new STDOUT

    FakeModel.configure hosts: ['127.0.0.1'], logger: @logger
    FakeModel.keyspace!
    FakeModel.table!

    @data = {
      uuid_field: 'myuuid',
      text_field: 'mytext',
      int_field: 33,
      double_field: 1.0,
      boolean_field: true,
      timestamp_field: Time.now,
      list_field: [
        {
          "key" => "value"
        },
        {
          "key2" => "value2"
        },
      ],
      set_field: Set.new([
        {
          "key" => "value"
        },
        {
          "key2" => "value2"
        },
      ]),
      set_text_field: ["a","b","c"],
      map_field: {
        {
          "key" => "value"
        } => {
          "key2" => "value2"
        },
      },
      map_text_field: {
        "key" => "value",
        "key2" => "value2"
      }
    }

    @data_with_nils = {
      uuid_field: 'myuuid',
      text_field: nil,
      int_field: nil,
      double_field: nil,
      boolean_field: nil,
      timestamp_field: nil,
      list_field: nil,
      set_field: nil,
      set_text_field: nil,
      map_field: nil,
      map_text_field: nil
    }
  end

  def teardown
    FakeModel.execute "DROP KEYSPACE #{FakeModel.keyspace};"
  end

  def test_model

    model = FakeModel.new @data

    assert_equal model.uuid_field, @data[:uuid_field]
    assert_equal model.text_field, @data[:text_field]
    assert_equal model.int_field, @data[:int_field]
    assert_equal model.double_field, @data[:double_field]
    assert_equal model.boolean_field, @data[:boolean_field]
    assert_equal model.timestamp_field, @data[:timestamp_field]
    assert_equal model.list_field, @data[:list_field]
    assert_equal model.set_field, @data[:set_field].to_set
    assert_equal model.set_text_field, @data[:set_text_field].to_set
    assert_equal model.map_field, @data[:map_field]
    assert_equal model.map_text_field, @data[:map_text_field]

    model.save
    model.save

    model2 = FakeModel.get uuid_field: 'pippo'
    assert_equal model2, nil

    model3 = FakeModel.get uuid_field: model.uuid_field
    assert model3
    assert_equal model3.uuid_field, @data[:uuid_field]
    assert_equal model3.text_field, @data[:text_field]
    assert_equal model3.int_field, @data[:int_field]
    assert_equal model3.double_field, @data[:double_field]
    assert_equal model3.boolean_field, @data[:boolean_field]
    assert_equal model3.timestamp_field.to_i, @data[:timestamp_field].to_i
    assert_equal model3.list_field, @data[:list_field]
    assert_equal model3.set_field, @data[:set_field]
    assert_equal model3.set_text_field, @data[:set_text_field].to_set
    assert_equal model3.map_field, @data[:map_field]
    assert_equal model3.map_text_field, @data[:map_text_field]

    model3.delete

    model4 = FakeModel.get uuid_field: model.uuid_field
    assert_equal model4, nil

  end

  def test_timestamp_as_integer
    now = Time.now
    ts = now.to_i
    model = FakeModel.new({timestamp_field: ts, uuid_field: 'myuuid'})
    assert model.timestamp_field.is_a?(Time), "timestamp_field should be a Time, is a #{model.timestamp_field.class}"
    model.save

    model2 = FakeModel.get uuid_field: model.uuid_field
    assert model2
    assert_equal model2.timestamp_field.to_i, ts
  end

  def test_timestamp_as_string
    now = Time.now
    ts = now.to_s
    model = FakeModel.new({timestamp_field: ts, uuid_field: 'myuuid'})
    assert model.timestamp_field.is_a?(Time), "timestamp_field should be a Time, is a #{model.timestamp_field.class}"
    model.save

    model2 = FakeModel.get uuid_field: model.uuid_field
    assert model2
    assert_equal model2.timestamp_field.to_s, ts
  end

  def test_if_not_exists
    assert_raises Cassandra::Errors::AlreadyExistsError do
      FakeModel.keyspace!
    end
    assert_raises Cassandra::Errors::AlreadyExistsError do
      FakeModel.keyspace!(if_not_exists: false)
    end
    FakeModel.keyspace!(if_not_exists: true)

    assert_raises Cassandra::Errors::AlreadyExistsError do
      FakeModel.table!
    end
    assert_raises Cassandra::Errors::AlreadyExistsError do
      FakeModel.table!(if_not_exists: false)
    end
    FakeModel.table!(if_not_exists: true)
  end

  def test_truncate
    FakeModel.truncate!  # should not fail truncating an empty table
    FakeModel.new(uuid_field: 'myuuid', text_field: "test").save
    model2 = FakeModel.get(uuid_field: 'myuuid')
    assert_not_nil model2
    FakeModel.truncate!
    model3 = FakeModel.get(uuid_field: 'myuuid')
    assert_equal model3, nil
  end

  def test_count
    FakeModel.new(uuid_field: 'myuuid', text_field: "test").save
    FakeModel.new(uuid_field: 'myuuid2', text_field: "test").save
    assert_equal 2, FakeModel.count
  end

  def test_to_h
    model = FakeModel.new @data
    @data[:set_text_field] = @data[:set_text_field].to_set
    assert_equal @data, model.to_h
  end

  def test_to_json
    model = FakeModel.new @data

    new_model = FakeModel.new(MultiJson.decode(model.to_json))
    assert_equal(@data[:uuid_field], new_model.uuid_field)
    assert_equal(@data[:text_field], new_model.text_field)
    assert_equal(@data[:int_field], new_model.int_field)
    assert_equal(@data[:double_field], new_model.double_field)
    assert_equal(@data[:boolean_field], new_model.boolean_field)
    assert_equal(@data[:timestamp_field].to_i, new_model.timestamp_field.to_i)
    assert_equal(@data[:list_field], new_model.list_field)
    assert_equal(@data[:set_field], new_model.set_field)
    assert_equal(@data[:set_text_field].to_set, new_model.set_text_field)
    assert_equal(@data[:map_text_field], new_model.map_text_field)
  end

  def test_each
    model = FakeModel.new @data
    @data[:set_text_field] = @data[:set_text_field].to_set
    assert(model.is_a?(Enumerable))
    assert(model.each.is_a?(Enumerator))
    model.each do |k, v|
      assert_equal(v, @data[k.to_sym])
    end
    assert_equal(model.map { |k, _| k }, @data.map { |k, _| k })
  end

  def test_exclude_nils_values
    model = FakeModel.new(@data)
    model.save
    model2 = FakeModel.new(@data_with_nils)
    model2.save(true)
    doc = FakeModel.execute(
      "SELECT * FROM #{FakeModel.keyspace}.#{FakeModel.table}"
    )
    assert doc
    assert_equal doc.to_a.count, 1
    doc.first.each do |k, v|
      assert v, "value in #{k} is nil"
    end
  end
end
