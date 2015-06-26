# encoding: utf-8

module Corm
  class GenericError < StandardError; end
  class TooManyKeysError < GenericError; end
  class UnknownPrimaryKey < GenericError; end
  class MissingPartitionKey < GenericError; end
  class MissingClusteringKey < GenericError; end
  class UnknownClusteringKey < GenericError; end
end
