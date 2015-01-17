module BeetleETL
  class AssignIds < Step

    def dependencies
      [TableDiff.step_name(table_name)].to_set
    end

    def run
      assign_new_ids
      map_existing_ids
    end

    def assign_new_ids
      database.execute <<-SQL
        UPDATE #{stage_table_name_sql}
        SET id = nextval('#{table_name}_id_seq')
        WHERE import_run_id = #{run_id}
        AND transition = 'CREATE'
      SQL
    end

    def map_existing_ids
      database.execute <<-SQL
        UPDATE #{stage_table_name_sql} stage
        SET id = public.id
        FROM #{public_table_name_sql} public
        WHERE stage.import_run_id = #{run_id}
        AND stage.transition IN ('KEEP', 'UPDATE', 'DELETE', 'UNDELETE')
        AND stage.external_id = public.external_id
      SQL
    end

  end
end
