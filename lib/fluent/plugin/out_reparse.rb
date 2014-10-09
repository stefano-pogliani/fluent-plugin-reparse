module Fluent
  class ReparseOutput < Output
    include Configurable

    Plugin.register_output("reparse", self)

    config_param :key, :string, :default => "message"
    config_param :tag, :string, :default => nil

    def configure(conf)
      super
      @parser = TextParser.new
      @parser.configure(conf)

      @key = conf["key"]
      @tag = conf["tag"]
    end

    def emit(tag, events_stream, chain)
      log.debug("Reparsing events.")
      es = MultiEventStream.new

      events_stream.each { |time, record|
        data = record[@key]
        @parser.parse(data) { |time, reparsed|
          unless reparsed.nil?
            unless reparsed.empty?
              reparsed.each { |key, value|
                record[key] = value
              }
            end
            es.add(time, record)
          end
        }
      }

      unless es.nil? or es.empty?
        unless @tag.nil?
          tag = @tag
        end

        begin
          Fluent::Engine.emit_stream(tag, es)
        rescue
          # ignore errors. Engine shows logs and backtraces.
        end
      end

      chain.next
    end

  end
end

