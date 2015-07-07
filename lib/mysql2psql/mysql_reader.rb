require 'rubygems'
require 'bundler/setup'

require 'mysql2'
#require 'mysql-pr'
require 'csv'

class Mysql2psql
  class MysqlReader
    class Field
    end

    class Table
      attr_reader :name

      def initialize(reader, name)
        @reader = reader
        @name = name
      end

      # @@types = %w(tiny enum decimal short long float double null timestamp longlong int24 date time datetime year set blob string var_string char).reduce({}) do |list, type|
      #   list[eval("::MysqlPR::Field::TYPE_#{type.upcase}")] = type
      #   list
      # end
      # @@types[246] = 'decimal'

      def columns
        @columns ||= load_columns
      end

      def convert_type(type)
        case type
        when /int.* unsigned/
          'bigint'
        when /bigint/
          'bigint'
        when 'bit(1)'
          'boolean'
        when 'tinyint(1)'
          'boolean'
        when /tinyint/
          'tinyint'
        when /int/
          'integer'
        when /varchar/
          'varchar'
        when /char/
          'char'
        when /decimal/
          'decimal'
        when /(float|double)/
          'double precision'
        when /set/
          'varchar'
        else
          type
        end
      end

      def list_fields
        result = @reader.mysql.list_fields(name)
        mysql_flags = ::MysqlPR::Field.constants.select { |c| c =~ /FLAG/ }
      end

      def load_columns
        @reader.reconnect
        # result = @reader.mysql.list_fields(name)
        # mysql_flags = ::MysqlPR::Field.constants.select { |c| c =~ /FLAG/ }
        fields = []
        res = @reader.query("EXPLAIN `#{name}`") 
        res.each do |field|
            puts "load_columns fields: #{field.inspect}"
            length = field[1][/\((\d+)\)/, 1] if field[1] =~ /\((\d+)\)/
            length = field[1][/\((\d+),(\d+)\)/, 1] if field[1] =~ /\((\d+),(\d+)\)/
            desc = {
              name: field[0],
              table_name: name,
              type: convert_type(field[1]),
              length: length && length.to_i,
              decimals: field[1][/\((\d+),(\d+)\)/, 2],
              null: field[2] == 'YES',
              primary_key: field[3] == 'PRI',
              auto_increment: field[5] == 'auto_increment'
            }
            desc[:default] = field[4] unless field[4].nil?
            puts "field: #{desc.inspect}"
            fields << desc
        end        

        fields.select { |field| field[:auto_increment] }.each do |field|
          field[:maxval] = @reader.query("SELECT max(`#{field[:name]}`) FROM `#{name}`").first.first.to_i                       
        end
        fields
      end
      def indexes
        load_indexes unless @indexes
        @indexes
      end

      def foreign_keys
        load_indexes unless @foreign_keys
        @foreign_keys
      end

      def load_indexes
        @indexes = []
        @foreign_keys = []

        result = @reader.query("SHOW CREATE TABLE `#{name}`")
        result.each do |row|
          explain = row[1]

          explain.split(/\n/).each do |line|
            next unless line =~ / KEY /
            index = {}
            if match_data = /CONSTRAINT `(\w+)` FOREIGN KEY \((.*?)\) REFERENCES `(\w+)` \((.*?)\)(.*)/.match(line)
              index[:name] = 'fk_' + name + '_' + match_data[1]
              index[:column] = match_data[2].gsub!('`', '').split(', ')
              index[:ref_table] = match_data[3]
              index[:ref_column] = match_data[4].gsub!('`', '').split(', ')

              the_rest = match_data[5]

              if match_data = /ON DELETE (SET NULL|SET DEFAULT|RESTRICT|NO ACTION|CASCADE)/.match(the_rest)
                index[:on_delete] = match_data[1]
              else
                index[:on_delete] ||= 'RESTRICT'
              end

              if match_data = /ON UPDATE (SET NULL|SET DEFAULT|RESTRICT|NO ACTION|CASCADE)/.match(the_rest)
                index[:on_update] = match_data[1]
              else
                index[:on_update] ||= 'RESTRICT'
              end

              @foreign_keys << index
            elsif match_data = /KEY `(\w+)` \((.*)\)/.match(line)
              index[:name] = 'idx_' + name + '_' + match_data[1]
              index[:columns] = match_data[2].split(',').map { |col| col[/`(\w+)`/, 1] }
              index[:unique] = true if line =~ /UNIQUE/
              @indexes << index
            elsif match_data = /PRIMARY KEY .*\((.*)\)/.match(line)
              index[:primary] = true
              index[:columns] = match_data[1].split(',').map { |col| col.strip.gsub(/`/, '') }
              @indexes << index
            end
          end
        end
      end

      def count_rows
        @reader.query("SELECT COUNT(*) FROM `#{name}`").first.first.to_i
      end

      def has_id?
        !!columns.find { |col| col[:name] == 'id' }
      end

      def count_for_pager
        query = has_id? ? 'MAX(id)' : 'COUNT(*)'
        res =@reader.query("SELECT #{query} FROM `#{name}`").first.first.to_i
      end

      def query_for_pager
        query = has_id? ? 'WHERE id >= ? AND id < ?' : 'LIMIT ?,?'

        cols = columns.map do |c|
          if "multipolygon" == c[:type]
            "AsWKT(`#{c[:name]}`) as `#{c[:name]}`"
          else
            "`#{c[:name]}`"
          end
        end

        "SELECT #{cols.join(", ")} FROM `#{name}` #{query}"
      end
    end

    def connect
      # @mysql = ::MysqlPR.connect(@host, @user, @passwd, @db, @port, @sock)
      # @mysql.charset = ::MysqlPR::Charset.by_number 192 # utf8_unicode_ci :: http://rubydoc.info/gems/mysql-pr/MysqlPR/Charset

      puts "Mysql2::Client #{@mysql_options.inspect}"
      @mysql = Mysql2::Client.new( @mysql_options )
      Mysql2::Client.default_query_options.merge!(:as => :array)
      @mysql.query('SET NAMES utf8')
      #@mysql.query('SET SESSION query_cache_type = OFF')
    end

    def reconnect
      @mysql.close rescue false
      connect
    end

    def query(*args)
      result = self.mysql.query(*args)
      puts "MySQL Query: #{args.inspect} ===> #{result.fields.inspect}"
      result
    rescue Mysql2::Error => e
      if e.message =~ /gone away/i
        self.reconnect
        retry
      else
        puts "MySQL Query failed '#{args.inspect}' #{e.inspect}"
        puts e.backtrace[0,5].join("\n")
        return []
      end
    end

    def initialize(options)
      @mysql_options = options.config['mysql']
      @host, @user, @passwd, @db, @port, @sock, @flag =
        options.mysqlhost('localhost'), options.mysqlusername,
        options.mysqlpassword, options.mysqldatabase,
        options.mysqlport(3306), options.mysqlsocket,
        options.mysqlflag
      @sock = nil if @sock == ''
      @flag = nil if @flag == ''
      connect
    end

    attr_reader :mysql

    def tables
      unless @tables
        @tables = [] 
        query("show tables").each do |row|        
          #puts "list_tables #{row.first.last}"
          name = row.first.last
          @tables << Table.new(self, name)
        end
      end
      @tables 
    end

    def paginated_read(table, page_size)
      count = table.count_for_pager
      return if count < 1
      statement = table.query_for_pager
      puts "paginated_read #{table.query_for_pager}"
      counter = 0
      0.upto((count + page_size) / page_size) do |i|
        v1 = i * page_size
        v2 = (table.has_id? ? (i + 1) * page_size : page_size)
        sql = statement.sub('?',v1.to_s).sub('?',v2.to_s)
        puts "paginated_read SQL #{sql}"
        res = @mysql.query(sql)
        res.each do |row|
          counter += 1
          yield(row, counter)
        end
      end
      counter
    end
  end
end