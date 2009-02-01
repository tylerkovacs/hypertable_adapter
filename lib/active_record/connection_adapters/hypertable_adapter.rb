require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_adapters/qualified_column'

module ActiveRecord
  class Base
    def self.require_hypertools
      # Include the hypertools driver if one hasn't already been loaded
      unless defined? Hypertable::ThriftClient
        gem 'hypertable-thrift-client'
        require_dependency 'thrift_client'
      end
    end

    def self.hypertable_connection(config)
      config = config.symbolize_keys
      config[:configuration_file] = "#{RAILS_ROOT}/#{config[:configuration_file]}" if config[:configuration_file][0,1] != '/'

      require_hypertools

      connection = Hypertable::ThriftClient.new(config[:host], config[:port])

      ConnectionAdapters::HypertableAdapter.new(connection, logger, config)
    end
  end

  module ConnectionAdapters
    class HypertableAdapter < AbstractAdapter
      @@read_latency = 0.0
      @@write_latency = 0.0
      cattr_accessor :read_latency, :write_latency

      def initialize(connection, logger, config)
        super(connection, logger)
        @config = config
        @hypertable_column_names = {}
      end

      def self.reset_timing
        @@read_latency = 0.0
        @@write_latency = 0.0
      end

      def self.get_timing
        [@@read_latency, @@write_latency]
      end

      def convert_select_columns_to_array_of_columns(s, columns=nil)
        select_rows = s.class == String ? s.split(',').map{|s| s.strip} : s
        select_rows = select_rows.reject{|s| s == '*'}

        if select_rows.empty? and !columns.blank?
          for c in columns
            next if c.name == 'ROW' # skip over the ROW key, always included
            if c.is_a?(QualifiedColumn)
              for q in c.qualifiers
                select_rows << qualified_column_name(c.name, q.to_s)
              end
            else
              select_rows << c.name
            end
          end
        end

        select_rows
      end

      def adapter_name
        'Hypertable'
      end

      def supports_migrations?
        true
      end

      def native_database_types
        {
          :string      => { :name => "varchar", :limit => 255 }
        }
      end

      def sanitize_conditions(options)
        case options[:conditions]
          when Hash
            options[:add_include_array] ||= []
            for key in options[:conditions].keys
              options[:add_include_array] << [key, [options[:conditions][key]].flatten]
            end
          when NilClass
            # do nothing
          else
            raise "Only hash conditions are supported"
        end
      end

      def execute_with_options(options)
        @connection.clear_conditions
        open_table(options[:table_name])

        # Rows can be specified using a number of different options:
        # row ranges (start_row and end_row)
        if options[:row_keys]
          options[:row_keys].flatten.each{|rk| @connection.add_row(rk)}
        elsif options[:start_row]
          raise "missing :end_row" if !options[:end_row]
          @connection.set_row_range(options[:start_row].to_s, options[:end_row])
        end

        # Default limit to 0 - which means return all rows
        options[:limit] ||= 0

        sanitize_conditions(options)

        if options[:add_include_array]
          for include_set in options[:add_include_array]
            column_name, include_values = include_set
            @connection.add_include_array(column_name.to_s, include_values)
          end
        end

        if options[:add_exclude_array]
          for exclude_set in options[:add_exclude_array]
            column_name, exclude_values = exclude_set
            @connection.add_exclude_array(column_name.to_s, exclude_values)
          end
        end

        select_rows = convert_select_columns_to_array_of_columns(options[:select], options[:columns])

        t1 = Time.now
        rows = @connection.full_select(select_rows, options[:limit])
        @@read_latency += Time.now - t1

        rows = convert_to_hashes(rows)
        rows
      end

      def convert_to_hashes(rows)
        # strip out nil values in the result set
        rows.map{|r| {'ROW' => r.first}.merge(r.last) }
      end

      def execute(hql, name=nil)
        log(hql, name) { @connection.run_hql(hql) }
      end

      # Returns array of column objects for table associated with this class.
      # Hypertable allows columns to include dashes in the name.  This doesn't
      # play well with Ruby (can't have dashes in method names), so we must
      # maintain a mapping of original column names to Ruby-safe names.
      def columns(table_name, name = nil)#:nodoc:
        # Each table always has a row key called 'ROW'
        columns = [
          Column.new('ROW', '')
        ]
        schema = describe_table(table_name)
        doc = REXML::Document.new(schema)
        column_families = doc.elements['Schema/AccessGroup[@name="default"]'].elements.to_a

        @hypertable_column_names[table_name] ||= {}
        for cf in column_families
          column_name = cf.elements['Name'].text
          rubified_name = rubify_column_name(column_name)
          @hypertable_column_names[table_name][rubified_name] = column_name
          columns << new_column(rubified_name, '')
        end

        columns
      end

      def remove_column_from_name_map(table_name, name)
        @hypertable_column_names[table_name].delete(rubify_column_name(name))
      end

      def add_column_to_name_map(table_name, name)
        @hypertable_column_names[table_name][rubify_column_name(name)] = name
      end

      def add_qualified_column(table_name, column_family, qualifiers=[], default='', sql_type=nil, null=true)
        qc = QualifiedColumn.new(column_family, default, sql_type, null)
        qc.qualifiers = qualifiers
        qualifiers.each{|q| add_column_to_name_map(table_name, qualified_column_name(column_family, q))}
        qc
      end

      def new_column(column_name, default_value='')
        Column.new(rubify_column_name(column_name), default_value)
      end

      def qualified_column_name(column_family, qualifier=nil)
        [column_family, qualifier].compact.join(':')
      end

      def rubify_column_name(column_name)
        column_name.to_s.gsub(/-+/, '_')
      end

      def is_qualified_column_name?(column_name)
        column_family, qualifier = column_name.split(':', 2)
        if qualifier
          [true, column_family, qualifier] 
        else
          [false, nil, nil]
        end
      end

      def quote(value, column = nil)
        case value
          when NilClass then ''
          when String then value
          else super(value, column)
        end
      end

      def quote_column_name(name)
        "'#{name}'"
      end

      def quote_column_name_for_table(name, table_name)
        quote_column_name(hypertable_column_name(name, table_name))
      end

      def hypertable_column_name(name, table_name, declared_columns_only=false)
        n = @hypertable_column_names[table_name][name]
        n ||= name if !declared_columns_only
        n
      end

      def describe_table(table_name)
        @connection.describe_table(table_name)
      end

      def tables(name=nil)
        @connection.show_tables
      end

      def drop_table(table_name, options = {})
        @connection.drop_table(table_name)
      end

      def write_cells(cells, table=nil)
        @connection.open_table(table) if table
        t1 = Time.now
        @connection.write_cells(cells)
        @@write_latency += Time.now - t1
      end

      def delete_cells(cells, table=nil)
        @connection.open_table(table) if table
        t1 = Time.now
        @connection.delete_cells(cells)
        @@write_latency += Time.now - t1
      end

      def delete_rows(row_keys, table=nil)
        @connection.open_table(table) if table
        t1 = Time.now
        @connection.delete_rows(row_keys)
        @@write_latency += Time.now - t1
      end

      def open_table(table_name)
        @connection.open_table(table_name)
      end

      def insert_fixture(fixture, table_name)
        fixture_hash = fixture.to_hash
        row_key = fixture_hash.delete('ROW')
        cells = []
        fixture_hash.keys.each{|k| cells << [row_key, k, fixture_hash[k]]}
        open_table(table_name)
        write_cells(cells)
      end

      private

        def select(hql, name=nil)
          # TODO: need hypertools run_hql to return result set
          raise "not yet implemented"
        end
    end
  end
end
