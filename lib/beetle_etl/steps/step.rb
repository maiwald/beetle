module BeetleETL

  DependenciesNotDefinedError = Class.new(StandardError)
  RunHookNotImplementedError = Class.new(StandardError)

  class Step

    Report = Struct.new(:started_at, :ended_at, :result) do
      def start_tracking
        @started_at = Time.now
      end

      def stop_tracking
        @started_at = Time.now
      end

      def result= (result)
        @result = result
      end
    end

    attr_reader :table_name, :report

    def initialize(table_name)
      @table_name = table_name
      @report = Report.new(table_name, name)
    end

    def self.step_name(table_name)
      "#{table_name}: #{name.split('::').last}"
    end

    def name
      self.class.step_name(table_name)
    end

    def dependencies
      raise DependenciesNotDefinedError
    end

    def run_hook
      raise RunHookNotImplementedError
    end

    def run
      @report.start_tracking
      @report.result = run_hook
      @report.stop_tracking
    end

    def run_id
      BeetleETL.state.run_id
    end

    def stage_schema
      BeetleETL.config.stage_schema
    end

    def external_source
      BeetleETL.config.external_source
    end

    def database
      BeetleETL.database
    end

  end
end
