require 'cassandra/uuid'
require 'corm'
require 'logger'
require 'test/unit'
require 'test_utils'
require 'securerandom'
require 'pry'

class TestModel < Test::Unit::TestCase
  include TestUtils

  def setup
    setup_corm!
  end

  def teardown
    teardown_corm!
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
    assert_false model.respond_to?(:ignore_me)
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
    assert_equal false, model3.respond_to?(:ignore_me)
    model3.delete
    model4 = FakeModel.get uuid_field: model.uuid_field
    assert_equal model4, nil
  end

  def test_table_does_not_ignore_fields_and_explode
    fme = FakeModelExplosive
    fme.keyspace!(if_not_exists: true)
    assert_raise Cassandra::Errors::SyntaxError do
      fme.table!(if_not_exists: true)
    end
  end

  def test_table_ignore_fields_and_does_not_explode
    fmd = FakeModelDefused
    fmd.keyspace!(if_not_exists: true)
    fmd.table!(if_not_exists: true)
    dt = {
      uuid_field: SecureRandom.hex(13),
      __id: SecureRandom.hex(13) }
    fmd.new(dt).save
    entry = fmd.get(uuid_field: dt[:uuid_field])
    assert(entry.respond_to?(:uuid_field))
    assert_false(entry.respond_to?(:__id))
  end

  def test_timestamp_as_integer
    now = Time.now
    ts = now.to_i
    model = FakeModel.new(timestamp_field: ts, uuid_field: 'myuuid')
    assert model.timestamp_field.is_a?(Time), "timestamp_field should be a Time, is a #{model.timestamp_field.class}"
    model.save

    model2 = FakeModel.get uuid_field: model.uuid_field
    assert model2
    assert_equal model2.timestamp_field.to_i, ts
  end

  def test_timestamp_as_string
    now = Time.now
    ts = now.to_s
    model = FakeModel.new(timestamp_field: ts, uuid_field: 'myuuid')
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
    FakeModel.new(uuid_field: 'myuuid', text_field: 'test').save
    model2 = FakeModel.get(uuid_field: 'myuuid')
    assert_not_nil model2
    FakeModel.truncate!
    model3 = FakeModel.get(uuid_field: 'myuuid')
    assert_equal model3, nil
  end

  def test_drop_table
    value = @some_random_keys.first
    model = FakeMultiKeyModel.new(@data.merge({ another_uuid_field: value }))
    model.save
    assert_equal(1, FakeMultiKeyModel.find.to_a.count)
    FakeMultiKeyModel.drop!
    FakeMultiKeyModel.table!
    assert_equal(0, FakeMultiKeyModel.find.to_a.count)
  end

  def test_count
    FakeModel.new(uuid_field: 'myuuid', text_field: 'test').save
    FakeModel.new(uuid_field: 'myuuid2', text_field: 'test').save
    assert_equal 2, FakeModel.count
  end

  def test_to_h
    dt = @data.clone
    model = FakeModel.new(dt)
    dt[:set_text_field] = dt[:set_text_field].to_set
    ignored = model.send(:ignored_fields).keys
    ignored.each { |k| dt.delete(k.to_sym) }
    assert_equal(dt, model.to_h)
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

  def test_each_1
    model = FakeModel.new @data
    @data[:set_text_field] = @data[:set_text_field].to_set
    assert(model.is_a?(Enumerable))
    assert(model.each.is_a?(Enumerator))
    model.each do |k, v|
      assert_equal(v, @data[k.to_sym])
    end
  end

  def test_each_2
    model = FakeModel.new @data
    @data[:set_text_field] = @data[:set_text_field].to_set
    ignored = model.send(:ignored_fields).keys
    ignored.each { |k| @data.delete(k.to_sym) }
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

  def test_find_count
    @some_random_keys.each do |a_value|
      model = FakeMultiKeyModel.new(@data.merge({ another_uuid_field: a_value }))
      model.save
    end

    assert_equal(4, FakeMultiKeyModel.find.count)
  end

  def test_find_count_with_limit
    @some_random_keys.each do |a_value|
      model = FakeMultiKeyModel.new(@data.merge({ another_uuid_field: a_value }))
      model.save
    end

    assert_equal(2, FakeMultiKeyModel.find(
      {},
      { limit: 2 }).count)
  end

  def test_find_with_block
    @some_random_keys.each do |a_value|
      model = FakeMultiKeyModel.new(@data.merge({ another_uuid_field: a_value }))
      model.save
    end

    another_uuid_fields = []
    FakeMultiKeyModel.find do |m|
      another_uuid_fields << m[:another_uuid_field]
    end

    assert_equal(@some_random_keys.sort, another_uuid_fields.sort)
    assert_equal(@some_random_keys.count, another_uuid_fields.count)
  end

  def test_find_with_all_required_keys
    @some_random_keys.each do |a_value|
      model = FakeMultiKeyModel.new(@data.merge({ another_uuid_field: a_value }))
      model.save
    end

    found_model_enumerator = FakeMultiKeyModel.find(
      uuid_field: @data[:uuid_field], # partition_key
      another_uuid_field: @some_random_keys.sort.first # clustering_key
    )

    assert_equal(1, found_model_enumerator.count)

    found_model = found_model_enumerator.next
    assert_equal(@data[:uuid_field], found_model[:uuid_field])
    assert_equal(@some_random_keys.sort.first, found_model[:another_uuid_field])
  end

  def test_find_by_partition_key
    found_model_enumerator = FakeMultiKeyModel.find(
      {
        uuid_field: @data[:uuid_field] # partition_key
        # empty clustering_key
      }
    )
    assert_equal(0, found_model_enumerator.count)

    @some_random_keys.each do |a_value|
      model = FakeMultiKeyModel.new(@data.merge({ another_uuid_field: a_value }))
      model.save
    end

    found_model_enumerator = FakeMultiKeyModel.find(
      { uuid_field: @data[:uuid_field] } # partition_key
    )
    assert_equal(4, found_model_enumerator.count)
  end

  def test_find_by_partition_key_with_limit
    @some_random_keys.each do |a_value|
      model = FakeMultiKeyModel.new(@data.merge({ another_uuid_field: a_value }))
      model.save
    end

    found_model_enumerator = FakeMultiKeyModel.find(
      { uuid_field: @data[:uuid_field] },
      { limit: 2 })

    assert_equal(2, found_model_enumerator.count)
  end

  def test_find_too_many_keys
    found_model_enumerator = FakeMultiKeyModel.find
    assert_equal(0, found_model_enumerator.count)

    @some_random_keys.each do |a_value|
      model = FakeMultiKeyModel.new(@data.merge({ another_uuid_field: a_value }))
      model.save
    end

    assert_raises Corm::TooManyKeysError do
      FakeMultiKeyModel.find(
        uuid_field: 0,
        another_uuid_field: 1,
        c: 2,
        d: 3
      ).next
    end
  end

  def test_find_missing_partition_key
    found_model_enumerator = FakeMultiKeyModel.find
    assert_equal(0, found_model_enumerator.count)

    @some_random_keys.each do |a_value|
      model = FakeMultiKeyModel.new(@data.merge({ another_uuid_field: a_value }))
      model.save
    end

    assert_raises Corm::MissingPartitionKey do
      FakeMultiKeyModel.find(
        { another_uuid_field: 1 }
      ).next
    end
  end

  def test_find_another_missing_partition_key_not_an_unknown_key
    found_model_enumerator = FakeMultiKeyModel.find
    assert_equal(0, found_model_enumerator.count)

    @some_random_keys.each do |a_value|
      model = FakeMultiKeyModel.new(@data.merge({ another_uuid_field: a_value }))
      model.save
    end

    assert_raises Corm::MissingPartitionKey do
      FakeMultiKeyModel.find(
        { wtf_uuid_field: 1 }
      ).next
    end
  end

  def test_find_missing_clustering_key
    found_model_enumerator = FakeMultiMultiKeyModel.find
    assert_equal(0, found_model_enumerator.count)

    @some_random_keys.each do |a_value|
      model = FakeMultiMultiKeyModel.new(
        @data.merge(
          {
            another_uuid_field: a_value,
            still_another_uuid_field: a_value
          }
        )
      )
      model.save
    end

    assert_raises Corm::MissingClusteringKey do
      FakeMultiMultiKeyModel.find(
        uuid_field: 0,
        still_another_uuid_field: 2
      ).next
    end
  end

  def test_find_unknown_clustering_key
    found_model_enumerator = FakeMultiMultiKeyModel.find
    assert_equal(0, found_model_enumerator.count)

    @some_random_keys.each do |a_value|
      model = FakeMultiMultiKeyModel.new(
        @data.merge(
          {
            another_uuid_field: a_value,
            still_another_uuid_field: a_value
          }
        )
      )
      model.save
    end
  end

  def test_find_if_there_are_no_results
    found_model_enumerator = FakeMultiKeyModel.find
    assert_equal(0, found_model_enumerator.count)

    @some_random_keys.each do |a_value|
      model = FakeMultiKeyModel.new(@data.merge({ another_uuid_field: a_value }))
      model.save
    end

    found_model_enumerator = FakeMultiKeyModel.find(
      uuid_field: 'you_wont_find_me'
    )

    assert_equal(0, found_model_enumerator.count)
    assert_raise StopIteration do
      found_model_enumerator.next
    end
    assert_equal([], found_model_enumerator.map { |a| a })
  end

  def test_find_if_is_given_a_key_of_a_different_type
    found_model_enumerator = FakeMultiKeyModel.find
    assert_equal(0, found_model_enumerator.count)

    @some_random_keys.each do |a_value|
      model = FakeMultiKeyModel.new(@data.merge({ another_uuid_field: a_value }))
      model.save
    end

    assert_raise ArgumentError do
      FakeMultiKeyModel.find({ uuid_field: :you_wont_find_me }).count
    end
    assert_raise ArgumentError do
      FakeMultiKeyModel.find(42).count
    end
  end

  def test_find_when_there_are_more_partition_keys
    # 4 entries for the uuid_field: 'myuuid'
    @some_random_keys.each do |a_value|
      model = FakeMultiKeyModel.new(@data.merge({ another_uuid_field: a_value }))
      model.save
    end

    # 2 entries for the uuid_field: 'not_my_uuuid'
    some_different_random_keys = %w(zero one)
    a_different_partition_key = { uuid_field: 'not_my_uuuid' }
    some_different_random_keys.each do |other_clustering_keys|
      model = FakeMultiKeyModel.new(
        @data.merge(
          { another_uuid_field: other_clustering_keys }
        ).merge(
          a_different_partition_key
        ))
      model.save
    end

    found_model_enumerator = FakeMultiKeyModel.find
    assert_equal(6, found_model_enumerator.count)

    found_model_enumerator = FakeMultiKeyModel.find(
      { uuid_field: @data[:uuid_field] } # partition_key
    )
    assert_equal(4, found_model_enumerator.count)

    found_model_enumerator = FakeMultiKeyModel.find(
      { uuid_field: a_different_partition_key[:uuid_field] } # partition_key
    )
    assert_equal(2, found_model_enumerator.count)

    found_model_enumerator = FakeMultiKeyModel.find(
      {
        uuid_field: a_different_partition_key[:uuid_field], # partition_key
        another_uuid_field: some_different_random_keys.sort.first # clustering_key
      }
    )
    assert_equal(1, found_model_enumerator.count)
    the_one_found = found_model_enumerator.first
    assert_equal(a_different_partition_key[:uuid_field], the_one_found[:uuid_field])
    assert_equal(some_different_random_keys.sort.first, the_one_found[:another_uuid_field])
  end
end
