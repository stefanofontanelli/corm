require 'corm'

module TestUtils
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

  class FakeMultiKeyModel < Corm::Model

    keyspace  :corm_test
    table     :corm_multi_key_model_test

    field :uuid_field,          :text
    field :another_uuid_field,  :text
    field :text_field,          :text
    field :int_field,           :int
    field :double_field,        :double
    field :boolean_field,       :boolean
    field :timestamp_field,     :timestamp
    field :list_field,          'list<JSON>'
    field :set_field,           'set<JSON>'
    field :set_text_field,      'set<TEXT>'
    field :map_field,           'map<JSON, JSON>'
    field :map_text_field,      'map<TEXT, TEXT>'

    primary_key [:uuid_field], :another_uuid_field
  end

  MODELS = [FakeModel, FakeMultiKeyModel]

  def setup_corm!
    @logger = Logger.new(STDOUT).tap { |logger| logger.level = Logger::INFO }

    MODELS.each do |model|
      model.configure(hosts: ['127.0.0.1'], logger: @logger)
      model.keyspace! rescue nil
      model.table!(if_not_exists: true)
    end

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

    @some_random_keys = %w[ foo bar lol meh ]
  end

  def teardown_corm!
    MODELS.each do |model_class|
      tablename = [ model_class.keyspace, model_class.table ].compact.join('.')
      model_class.cluster.connect.tap do |connection|
        connection.execute("TRUNCATE #{tablename};") rescue nil
        connection.execute("DROP TABLE IF EXISTS #{tablename};")
        connection.execute("DROP KEYSPACE IF EXISTS #{model_class.keyspace};")
        connection.close
      end
    end
  end
end
