require 'spec_helper'

module BeetleETL
  describe Transform do

    let(:database) { double(:database) }
    let(:config) do
      OpenStruct.new({
        database: database
      })
    end
    let(:query) { double(:query) }

    subject do
      deps = [:some_table, :some_other_table].to_set
      Transform.new(config, :example_table, deps, query)
    end

    describe '#dependencies' do
      it 'depends on Transform of all dependencies' do
        expect(subject.dependencies).to eql(
          [
            'some_table: Transform',
            'some_other_table: Transform',
          ].to_set
        )
      end
    end

    describe '#run' do
      it 'runs a query in the database' do
        expect(database).to receive(:execute).with(query)

        subject.run
      end
    end

  end
end
