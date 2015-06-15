# encoding: utf-8

require 'cassandra'
require 'corm/exceptions'
require 'multi_json'
require 'set'
require 'digest/md5'

module Corm
  class Model
    include Enumerable

    @@cluster = nil

    def self.configure(opts = {})
      @@cluster = Cassandra.cluster(opts)
    end

    def self.cluster
      @@cluster
    end

    def self.execute(*args)
      session.execute(*args)
    end

    def self.field(name, type, pkey = false)
      fields[name.to_s.downcase] = type.to_s.downcase
      primary_key name.to_s.downcase if pkey

      send :define_method, name.to_s.downcase do
        type = fields[name.to_s.downcase].to_s.downcase
        value = record[name.to_s.downcase]
        if type == 'json'
          value.nil? ? nil : MultiJson.decode(value)
        elsif type.start_with?('list') && type['json']
          value.nil? ? [] : value.map { |s| MultiJson.decode(s) }
        elsif type.start_with?('list')
          value.nil? ? [] : value
        elsif type.start_with?('set') && type['json']
          Set.new(value.nil? ? [] : value.map { |s| MultiJson.decode(s) })
        elsif type.start_with?('set')
          Set.new(value.to_a)
        elsif type.start_with?('map') && type['json']
          hash = {}
          (value || {}).each do |k, v|
            k = MultiJson.decode(k) if type['json,'] || type['json ,']
            v = MultiJson.decode(v) if type[', json'] || type[',json']
            hash[k] = v
          end
          hash
        elsif type.start_with?('map')
          value.nil? ? {} : value
        else
          value
        end
      end

      send :define_method, '[]' do |field|
        send(field.to_s.downcase)
      end

      send :define_method, "#{name.to_s.downcase}=" do |value|
        type = fields[name.to_s.downcase].to_s.downcase
        @raw_values[name.to_s.downcase] = value
        val = if type == 'json'
                value.to_s.empty? ? nil : MultiJson.encode(value)
              elsif type.start_with?('list') && type['json']
                value.to_a.empty? ? [] : value.map { |s| MultiJson.encode(s) }
              elsif type.start_with?('list')
                value.nil? ? [] : value
              elsif type.start_with?('set') && type['json']
                Set.new(value.nil? ? [] : value.map { |s| MultiJson.encode(s) })
              elsif type.start_with?('set')
                Set.new(value.to_a)
              elsif type.start_with?('map') && type['json']
                hash = {}
                (value || {}).each do |k, v|
                  k = MultiJson.encode(k) if type['json,'] || type['json ,']
                  v = MultiJson.encode(v) if type[', json'] || type[',json']
                  hash[k] = v
                end
                hash
              elsif type.start_with?('map')
                value.nil? ? {} : value
              elsif type == ('timestamp')
                if value.is_a?(Fixnum)
                  Time.at(value)
                elsif value.is_a?(String)
                  Time.parse(value)
                else
                  value
                end
              else
                value
              end
        record[name.to_s.downcase] = val
      end

      send :define_method, '[]=' do |field, value|
        send("#{field.to_s.downcase}=", value)
      end

      nil
    end

    def to_h
      Hash[collect { |k, v| [k, v] }]
    end

    alias_method :to_hash, :to_h

    def to_json
      res = to_h
      fields.each do |k, t|
        res[k.to_sym] = res[k.to_sym].to_a if t.start_with?('set')
      end
      MultiJson.encode(res)
    end

    def each(&block)
      return enum_for(:each) unless block_given?
      fields.keys.each { |k| block.call([k.to_sym, send(k.to_sym)]) }
    end

    def self.fields
      class_variable_set(
        :@@fields,
        {}
      ) unless class_variable_defined?(:@@fields)
      class_variable_get(:@@fields)
    end

    def self.count
      if statements['count'].nil?
        statements['count'] = session.prepare(
          "SELECT COUNT(*) FROM #{[keyspace, table].compact.join '.'};"
        )
      end
      execute(statements['count']).first['count'].to_i
    end

    def self.drop!
      execute("DROP TABLE #{[keyspace, table].compact.join '.'};")
    end

    ##
    # Find by keys.
    # This `find` methods wants to be as flexible as possible.
    #
    # Unless a block is given, it returns an `Enumerator`, otherwise it yields
    # to the block an instance of the found Cassandra entries.
    #
    # If no keys as passed as parameter, the methods returns (an Enumerator for)
    # all the results in the table.
    #
    # If one or more keys are given, the **order matters**; since this methods
    # will craft a CSQL query, the first key **must** be the `partition key`,
    # while the followings are intended as the `clustering keys`... that can be
    # more than one.
    #
    # If the keys passed as parameter are more than the defined by the table the
    # query is not valid, it cannot be executed and an error is raised.
    def self.find(*key_values, &block)
      return to_enum(:find, *key_values) unless block_given?

      statement_find_key = ["find"]
      field_names = []
      Array(key_values).each_with_index do |value, index|
        statement_find_key << primary_key.flatten[index].to_s
        field_names << "#{primary_key.flatten[index]} = ?"
      end

      statement_find_key = statement_find_key.join('_')

      if statements[statement_find_key].nil?
        statement = self.the_find_statement_for(key_values, field_names)
        statements[statement_find_key] = session.prepare(statement)
      end

      execute(statements[statement_find_key], arguments: key_values).each do |cassandra_record_|
        block.call(new(_cassandra_record: cassandra_record_))
      end
    end

    def self.get(relations)
      if statements['get'].nil?
        fields = primary_key.flatten.map { |key| "#{key} = ?" }.join ' AND '
        statement = "SELECT * FROM #{keyspace}.#{table} WHERE #{fields} LIMIT 1;"
        statements['get'] = session.prepare statement
      end
      values = primary_key.flatten.map do |key|
        relations[key.to_s] || relations[key.to_sym]
      end
      cassandra_record_ = execute(statements['get'], arguments: values).first
      cassandra_record_ ? new(_cassandra_record: cassandra_record_) : nil
    end

    def self.keyspace(name = nil)
      class_variable_set(:@@keyspace, name.to_s) unless name.nil?
      class_variable_get(:@@keyspace)
    end

    def self.keyspace!(opts = {})
      replication = opts[:replication] ||
                    "{'class': 'SimpleStrategy', 'replication_factor': '1'}"
      durable_writes = opts[:durable_writes].nil? ? true : opts[:durable_writes]
      if_not_exists = opts[:if_not_exists] ? 'IF NOT EXISTS' : ''
      cluster.connect.execute(
        "CREATE KEYSPACE #{if_not_exists} #{keyspace} WITH replication = #{replication} AND durable_writes = #{durable_writes};"
      )
    end

    def self.primary_key(partition_key = nil, *cols)
      class_variable_set(
        :@@primary_key,
        [Array(partition_key), cols]
      ) unless partition_key.nil?
      class_variable_get(:@@primary_key)
    end

    def self.properties(*args)
      class_variable_set(
        :@@properties,
        args
      ) unless class_variable_defined?(:@@properties)
      class_variable_get(:@@properties)
    end

    def self.session
      class_variable_set(
        :@@session,
        cluster.connect(keyspace)
      ) unless class_variable_defined?(:@@session)
      class_variable_get :@@session
    end

    def self.statements
      class_variable_set(
        :@@statements,
        {}
      ) unless class_variable_defined?(:@@statements)
      class_variable_get(:@@statements)
    end

    def self.table(name = nil)
      class_variable_set(:@@table, name) unless name.nil?
      class_variable_get(:@@table)
    end

    def self.table!(opts = {})
      if_not_exists = opts[:if_not_exists] ? 'IF NOT EXISTS' : ''
      table_ = [keyspace, table].compact.join('.')
      pkey = []
      partition_key = primary_key[0].join(',')
      pkey << (
        primary_key[0].count > 1 ? "(#{partition_key})" : partition_key
      ) unless primary_key[0].to_a.empty?
      pkey << primary_key[1].join(',') unless primary_key[1].to_a.empty?
      pkey = pkey.join(',')
      fields_ = fields.to_a.map { |a| a.join(' ') }.concat(["PRIMARY KEY (#{pkey})"]).join(', ')
      definition = "CREATE TABLE #{if_not_exists} #{table_} (#{fields_})".downcase.gsub('json', 'text')
      definition = properties.to_a.empty? ? "#{definition};" : "#{definition} WITH #{properties.to_a.join ' AND '};"
      execute(definition)
    end

    def self.drop_table!
      execute("DROP TABLE IF EXISTS #{[keyspace, table].compact.join('.')};")
    end

    def self.truncate!
      execute("TRUNCATE #{[keyspace, table].compact.join '.'};")
    end

    def initialize(opts = {})
      @record = opts.delete(:_cassandra_record) ||
                opts.delete('_cassandra_record')
      @raw_values = {}
      opts.each { |k, v| send("#{k}=", v) } if @record.nil?
    end

    def delete
      if statements['delete'].nil?
        pks = primary_key.flatten.map { |key| "#{key} = ?" }.join(' AND ')
        statement = "DELETE FROM #{keyspace}.#{table} WHERE #{pks};"
        statements['delete'] = session.prepare(statement)
      end
      values = primary_key.flatten.map do |key|
        record[key.to_s] || record[key.to_sym]
      end
      execute(statements['delete'], arguments: values)
      nil
    end

    def record
      @record ||= {}
    end

    def save(exclude_nil_values = false)
      keys = fields.keys.map do |k|
        v = !exclude_nil_values || @raw_values.empty? ? record[k] : @raw_values[k]
        exclude_nil_values && v.nil? ? nil : k
      end.compact
      return nil if keys.empty?
      execute(
        session.prepare(
          "INSERT INTO #{keyspace}.#{table} (#{keys.join(',')}) VALUES (#{keys.map { '?' }.join(',')});"
        ),
        arguments: keys.map { |k| record[k] }
      )
      nil
    end

    protected

    def execute(*args)
      self.class.execute(*args)
    end

    def keyspace
      self.class.keyspace
    end

    def fields
      self.class.fields
    end

    def primary_key
      self.class.primary_key
    end

    def session
      self.class.session
    end

    def statements
      self.class.statements
    end

    def table
      self.class.table
    end

    ##
    # Create and return the proper query to find the C* entries, given an array
    # of keys.
    #
    # @param key_values An array of key names; can be empty, cannot be, in size, greater than the length of the model keys.
    # @param field_names The "column names" for the `WHERE` clause.
    def self.the_find_statement_for(key_values, field_names)
      if key_values.empty?
        return "SELECT * FROM #{keyspace}.#{table} ;"
      elsif key_values.count > primary_key.flatten.count
        raise Corm::TooManyKeysError
      else
        return "SELECT * FROM #{keyspace}.#{table} WHERE #{field_names.join(' AND ')} ;"
      end
    end
  end
end
