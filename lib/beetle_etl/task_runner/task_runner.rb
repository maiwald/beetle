module BeetleETL
  class TaskRunner

    def initialize(tasks)
      @dependency_resolver = DependencyResolver.new(tasks)
      @tasks = tasks

      @queue = Queue.new
      @results = {}
    end

    def run
      Thread.new do
        until all_run?
          runnables.each do |task|
            mark_running(task.name)
            run_task_async(task)
          end

          task, result = @queue.pop
          mark_completed(task, result)
        end
      end.join

      @results
    end

    private

    def run_task_async(task)
      Thread.new do
        @queue.push [task.name, task.run]
      end
    end

    def mark_completed(task_name, result)
      @results[task_name][:ended_at] = now
      @results[task_name][:result] = result
    end

    def mark_running(task_name)
      @results[task_name] = { started_at: now }
    end

    def runnables
      resolvables.reject { |r| running.include? r.name }
    end

    def resolvables
      @dependency_resolver.resolvables(completed)
    end

    def running
      @results.map do |table_name, task_data|
        table_name unless task_data.has_key? :ended_at
      end.compact
    end

    def completed
      @results.map do |table_name, task_data|
        table_name if task_data.has_key? :ended_at
      end.compact
    end

    def all_run?
      @tasks.map(&:name).to_set == completed.to_set
    end

    def now
      Time.now
    end

  end
end
