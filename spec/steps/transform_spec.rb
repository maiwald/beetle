require 'spec_helper'

module Beetle
  describe Transform do

    let(:query) { double(:query) }
    subject do
      deps = [:some_table, :some_other_table].to_set
      Transform.new(:example_table, deps, query)
    end

    describe '#dependencies' do
      it 'depends on Transform of all dependencies' do
        expect(subject.dependencies).to eql(
          [
            'some_table: Beetle::Transform',
            'some_other_table: Beetle::Transform',
          ].to_set
        )
      end
    end

    describe '#run' do
      it 'runs a query in the database' do
        database = double(:database)
        Beetle.configure { |config| config.database = database }

        expect(database).to receive(:run).with(query)

        subject.run
      end
    end
  end
end
