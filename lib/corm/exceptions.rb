# encoding: utf-8

module Corm
  class TooManyKeysError < StandardError; end
  class UnknownKey < StandardError; end
  class MissingPartitionKey < StandardError; end
  class MissingClusteringKey < StandardError; end
  class UnknownClusteringKey < StandardError; end
end
