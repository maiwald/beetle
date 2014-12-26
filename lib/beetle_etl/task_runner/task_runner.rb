module BeetleETL
  class TaskRunner

    def initialize(tasks)
      @dependency_resolver = DependencyResolver.new(tasks)
      @tasks = tasks

      @queue = Queue.new
      @completed = Set.new
      @running = Set.new
    end

    def run
      Thread.new do
        until all_run?
          runnables.each do |task|
            mark_running(task.name)
            run_task_async(task)
          end

          mark_completed(@queue.pop)
        end
      end.join
    end

    private

    def run_task_async(task)
      Thread.new do
        task.run
        @queue.push task.name
      end
    end

    def mark_completed(task_name)
      @running.delete(task_name)
      @completed << task_name
    end

    def mark_running(task_name)
      @running << task_name
    end

    def runnables
      resolvables.reject { |r| @running.include? r.name }
    end

    def resolvables
      @dependency_resolver.resolvables(@completed)
    end

    def all_run?
      @tasks.map(&:name).all? { |name| @completed.include? name }
    end

  end
end
