# encoding: utf-8

module Corm
  module Validations
    def clustering_key_missing?(params)
      return false if no_clustering_key?(params)
      keys = clustering_key.map(&:to_sym) - params.keys.map(&:to_sym)
      keys.empty? ? false : true
    end

    def no_clustering_key?(params)
      (params.keys.map(&:to_sym) - partition_key.flatten.map(&:to_sym)).empty?
    end

    def partition_key_missing?(params)
      keys = partition_key.map(&:to_sym) - params.keys.map(&:to_sym)
      keys.empty? ? false : true
    end

    def too_many_keys?(params)
      params.keys.count > primary_key_count ? true : false
    end

    def unknown_primary_key?(params)
      (params.keys.map(&:to_sym) - primary_key.flatten.map(&:to_sym)).empty? ? false : true
    end

    def validate_query(params, opts)
      if !params.is_a?(Hash)
        error = ArgumentError
        message = "'params' argument must be an hash: #{params}"
      elsif !opts.is_a?(Hash)
        error = ArgumentError
        message = "'opts' argument must be an hash: #{opts}"
      elsif too_many_keys?(params)
        error = TooManyKeysError
        message = "#{params.keys}"
      elsif !params.empty? && partition_key_missing?(params)
        error = MissingPartitionKey
        message = "#{params.keys}"
      elsif !params.empty? && clustering_key_missing?(params)
        error = MissingClusteringKey
        message = "#{params.keys}"
      elsif !params.empty? && unknown_primary_key?(params)
        error = UnknownPrimaryKey
        message = "#{params.keys}"
      else
        return
      end
      fail(error, message, caller)
    end
  end
end
