# encoding: utf-8

require 'cassandra'
require 'multi_json'
require 'set'

module Corm
  class Model
    include Enumerable

    @cluster = nil

    def self.configure(opts = {})
      @cluster = Cassandra.cluster(opts)
    end

    def self.cluster
      @cluster
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

    def self.truncate!
      execute("TRUNCATE #{[keyspace, table].compact.join '.'};")
    end

    def initialize(opts = {})
      @record = opts.delete(:_cassandra_record) ||
                opts.delete('_cassandra_record')
      @opts = opts
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

    def save(exclude_nil_values = false, use_raw_values = false)
      keys = fields.keys.map do |k|
        value = use_raw_values ? (opts[k.to_s] || opts[k.to_sym]) : record[k]
        exclude_nil_values && value.nil? ? nil : k
      end.compact
      execute(
        session.prepare(
          "INSERT INTO #{keyspace}.#{table} (#{keys.join(',')}) VALUES (#{keys.map { '?' }.join(',')});"
        ),
        arguments: keys.map do |k|
          use_raw_values ? (opts[k.to_s] || opts[k.to_sym]) : record[k]
        end
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
  end
end
