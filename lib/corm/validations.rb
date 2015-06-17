# encoding: utf-8

module Corm
  module Validations

    def there_are_too_many_keys_requested?(key_values)
      if key_values.keys.count > self.primary_key_count
        return "You defined more find keys than the primary keys of the table!"
      end
      false
    end

    def an_unknown_key_is_requested?(key_values)
      unless (key_values.keys - self.primary_key.flatten).empty?
        return "You requested a key that it's not in the table primary key!"
      end
      false
    end

    def an_unknown_clustering_key_is_requested?(key_values)
      unless ((key_values.keys - self.partition_key.flatten) - self.clustering_key).empty?
        return "You requested some unsupported clustering keys!"
      end
      false
    end

    def a_partition_key_is_missing?(key_values)
      self.partition_key.each do |part_key|
        return "#{part_key} is required as partition key!" unless key_values.keys.include?(part_key.to_sym)
      end
      false
    end

    def a_clustering_key_is_missing?(key_values)

      # This exception mimic the following...
      # Class: <Cassandra::Errors::InvalidError>
      # Message: <"PRIMARY KEY column 'still_another_uuid_field' cannot be
      # restricted (preceding column 'another_uuid_field' is either not
      # restricted or by a non-EQ relation)"
      #
      # ... and TBH leaving this check to the Cassandra driver is still an
      # option.
      return false if self.clustering_key.empty?
      return false if no_clustering_key_requested?(key_values)
      self.clustering_key.each do |clust_key|
        return "#{clust_key} is required as clustering key! (Order matters)" unless key_values.include?(clust_key.to_sym)
      end
      false
    end

    def no_clustering_key_requested?(key_values)
      (key_values.keys - self.partition_key.flatten).empty?
    end
  end
end
