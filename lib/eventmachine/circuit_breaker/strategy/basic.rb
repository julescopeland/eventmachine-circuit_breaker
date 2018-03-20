module EventMachine
  module CircuitBreaker
    module Strategy
      class Basic
        def initialize(failure_limit: 5, recovery_time: 30, recovery_ratio: 0.1, log_proc: nil)
          @mutex = EventMachine::CircuitBreaker::RWMutex.new
          @recovery_time = recovery_time
          @state = State.new(
            :failure_limit  => failure_limit,
            :recovery_ratio => recovery_ratio,
            :log_proc       => log_proc || method(:log),
            :on_open        => method(:on_open),
          )
        end

        def open?(_client = nil)
          mutex.read_lock { @state.open? }
        end

        def handle_response(client)
          err = client.response_header.server_error?

          mutex.write_lock do
            @state.client = client
            err ? @state.handle_failure : @state.handle_success
          end
        end

        def status_report
          {
            :status => open? ? "OPEN" : "CLOSED",
          }
        end

        def open!
          mutex.write_lock { @state.open! }
        end

        private

        attr_reader :mutex

        # Callback for state - called after the circuit breaker is opened
        def on_open
          log(
            :level   => :info,
            :message => "Will set circuit to half-open in #{@recovery_time} seconds",
          )
          EventMachine.add_timer(@recovery_time) do
            mutex.write_lock { @state.handle_recovery_timer }
          end
        end

        def log(level: :info, client: nil, message: '')
          request = client&.req
          host = request&.host || nil
          path = request&.path || nil

          puts "#{level.to_s.upcase}: Circuit breaker[#{host}][#{path}]\n\t#{message}"
        end

        class State
          attr_reader :log_proc
          attr_accessor :client

          def initialize(failure_limit:, recovery_ratio:, log_proc: nil, on_open: nil)
            @failure_limit = failure_limit
            @recovery_ratio = recovery_ratio
            @log_proc = log_proc
            @on_open = on_open

            @state = :closed
            @failures = 0
          end

          # Circuit Closed - default state - all good
          def closed?
            @state == :closed
          end

          # Circuit Open - there were too many failures, rejecting all requests for a while
          def open?
            @state == :open
          end

          # Circuit was open, giving it a chance to recover
          def half_open?
            @state == :half_open
          end

          def close!
            @state = :closed
            @failures = 0
            log(
              :level   => :info,
              :message => "Closed. Failure count: #{@failures}",
            )
          end

          def open!
            return if open?
            @state = :open
            log(
              :level   => :error,
              :message => "Opening circuit!",
            )
            @on_open.call if @on_open
          end

          def half_open!
            @state = :half_open
            log(
              :level   => :info,
              :message => "Half-open",
            )
          end

          def handle_failure
            increment_failure_counter
          end

          def increment_failure_counter
            @failures += 1
            log(
              :level   => :warn,
              :message => "Failure. Counter: #{@failures.round(2)}",
            )
            open! if @failures >= @failure_limit
          end

          def handle_success
            decrement_failure_counter
            close! if half_open?
          end

          def decrement_failure_counter
            if @failures > 0
              @failures -= @recovery_ratio
              log(
                :level   => :info,
                :message => "Success. Counter: #{@failures.round(2)}",
              )
            end
          end

          def handle_recovery_timer
            half_open!
          end

          def log(level:, message:)
            if @log_proc
              @log_proc.call(
                :level   => level,
                :client  => client,
                :message => message,
              )
            end
          end
        end
      end
    end
  end
end
