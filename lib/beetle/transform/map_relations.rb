module Beetle
  class MapRelations

    include CommonHelpers

    attr_reader :table_name, :dependencies

    def initialize(table_name, dependencies)
      @table_name = table_name
      @dependencies = dependencies
    end

    def run
      dependencies.each do |foreign_key_column, foreign_table_name|
        database.from(
          :"#{stage_schema}__#{table_name}___ST",
          :"#{stage_schema}__#{foreign_table_name}___FT"
        ).where(
          ST__import_run_id: run_id,
          FT__import_run_id: run_id,
          FT__external_id: :"ST__external_#{foreign_key_column}",
        ).update(
          :"#{foreign_key_column}" => :"FT__id"
        )
      end
    end

  end
end
