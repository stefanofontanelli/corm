# encoding: utf-8

module Corm
  module Enhancements

    def enhancements_logger(logger = nil)
      Logger.new(STDOUT).tap { |l| l.level = Logger::INFO } unless logger
    end

    RESCUED_CASSANDRA_EXCEPTIONS = [
      ::Cassandra::Errors::ExecutionError,
      ::Cassandra::Errors::IOError,
      ::Cassandra::Errors::InternalError,
      ::Cassandra::Errors::NoHostsAvailable,
      ::Cassandra::Errors::ServerError,
      ::Cassandra::Errors::TimeoutError
    ]

    # Trying to rescue from a Cassandra::Error
    #
    # The relevant documentation is here (version 2.1.3):
    # https://datastax.github.io/ruby-driver/api/error/
    #
    # Saving from:
    #
    # - ::Cassandra::Errors::ExecutionError
    # - ::Cassandra::Errors::IOError
    # - ::Cassandra::Errors::InternalError
    # - ::Cassandra::Errors::NoHostsAvailable
    # - ::Cassandra::Errors::ServerError
    # - ::Cassandra::Errors::TimeoutError
    #
    # Ignoring:
    # - Errors::ClientError
    # - Errors::DecodingError
    # - Errors::EncodingError
    # - Errors::ValidationError
    #
    # A possible and maybe-good refactoring could be refine for the
    # network related issues.
    def attempts_wrapper(attempts = 3, &block)
      (1..attempts).each do |i|
        begin
          return block.call() if block_given?
        rescue *RESCUED_CASSANDRA_EXCEPTIONS => e
          enhancements_logger.error { "(#{i}/#{attempts} attempts) Bad fail! Retry in #{i*2} seconds to recover  #{e.class.name}: #{e.message}" }
          sleep(i*2)
        end
      end
      nil
    end
  end
end
