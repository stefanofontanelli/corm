# encoding: utf-8

require 'cassandra'
require 'multi_json'
require 'set'


module Corm

  class Model

    def self.configure opts = {}
      @cluster = Cassandra.cluster opts
    end

    def self.cluster
      @cluster ||= nil
      @cluster
    end

    def self.execute *args
      session.execute *args
    end

    def self.field name, type, pkey = false
      
      fields[name.to_s.downcase] = type.to_s.downcase
      
      primary_key name.to_s.downcase if pkey
      
      send(:define_method, name.to_s.downcase) do
        type = self.class.fields[name.to_s.downcase]
        value = record[name.to_s.downcase]
        if type == 'json'
          MultiJson.decode value
        elsif type.start_with?('list') && type['json']
          value.map{|s| MultiJson.decode s}
        elsif type.start_with?('set') && type['json']
          Set.new value.map{|s| MultiJson.decode s}
        elsif type.start_with?('map') && type['json']
          value.inject({}) do |hash, (k, v)|
            k = MultiJson.decode k if type['json,'] || type['json ,']
            v = MultiJson.decode v if type[', json'] || type[',json']
            hash.merge(k => v)
          end
        else
          value
        end
      end

      send(:define_method, '[]') do |field|
        send field.to_s.downcase
      end

      send(:define_method, "#{name.to_s.downcase}=") do |value|
        type = self.class.fields[name.to_s.downcase]
        record[name.to_s.downcase] = if type == 'json'
          MultiJson.encode value
        elsif type.start_with?('list') && type['json']
          value.map{|s| MultiJson.encode s}
        elsif type.start_with?('set') && type['json']
          Set.new value.map{|s| MultiJson.encode s}
        elsif type.start_with?('map') && type['json']
          value.inject({}) do |hash, (k, v)|
            k = MultiJson.encode k if type['json,'] || type['json ,']
            v = MultiJson.encode v if type[', json'] || type[',json']
            hash.merge(k => v)
          end
        else
          value
        end
      end

      send(:define_method, '[]=') do |field, value|
        send "#{field.to_s.downcase}=", value
      end

      nil
    end

    def self.fields
      @fields ||= {}
      @fields
    end

    def self.get relations
      if statements['get'].nil?
        fields = primary_key.flatten.map{ |key| "#{key} = ?" }.join ' AND '
        statement = "SELECT * FROM #{keyspace}.#{table} WHERE #{fields} LIMIT 1;"
        statements['get'] = session.prepare statement
      end
      values = primary_key.flatten.map{ |key| relations[key.to_s] || relations[key.to_sym] }
      _cassandra_record = execute(statements['get'], arguments: values).first
      _cassandra_record ? self.new(_cassandra_record: _cassandra_record) : nil
    end

    def self.keyspace name = nil
      @keyspace = name.to_s unless name.nil?
      @keyspace
    end

    def self.primary_key partition_key = nil, *cols
      @primary_key = [Array(partition_key), cols] unless partition_key.nil?
      @primary_key
    end

    def self.properties *args
      @properties ||= args
      @properties
    end

    def self.session
      @session ||= cluster.connect keyspace
      @session
    end

    def self.statements
      @statements ||= {}
      @statements
    end

    def self.table name = nil
      @table = name unless name.nil?
      @table
    end

    def self.table!
      table_ = [keyspace, table].compact.join '.'
      pkey = []
      partition_key = primary_key[0].join(',')
      pkey << (primary_key[0].count > 1 ? "(#{partition_key})" : partition_key) unless primary_key[0].to_a.empty?
      pkey << primary_key[1].join(',') unless primary_key[1].to_a.empty?
      pkey = pkey.join ','
      fields_ = fields.to_a.map{ |args| args.join ' ' }.concat(["PRIMARY KEY (#{pkey})"]).join ', '
      definition = "CREATE TABLE #{table_} (#{fields_})".downcase.gsub('json', 'text')
      definition = properties.to_a.empty? ? "#{definition};" : "#{definition} WITH #{properties.to_a.join ' AND '};"
      execute definition
    end

    def initialize opts = {}
      @record = opts.delete(:_cassandra_record) || opts.delete('_cassandra_record')
      opts.each{ |k,v| self.send "#{k}=", v } if @record.nil?
    end

    def delete
      if statements['delete'].nil?
        pks = primary_key.flatten.map{ |key| "#{key} = ?" }.join ' AND '
        statement = "DELETE FROM #{keyspace}.#{table} WHERE #{pks};"
        statements['delete'] = session.prepare statement
      end
      values = primary_key.flatten.map{ |key| record[key.to_s] || record[key.to_sym] }
      execute statements['delete'], arguments: values
      nil
    end

    def record
      @record ||= {}
      @record
    end

    def save
      if statements['save'].nil?
        values = fields.keys.map{'?'} .join ','
        statement = "INSERT INTO #{keyspace}.#{table} (#{fields.keys.join ','}) VALUES (#{values});"
        statements['save'] = session.prepare statement
      end
      execute statements['save'], arguments: fields.keys.map{|k| record[k]}
      nil
    end

    private

      def execute *args
        self.class.execute *args
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