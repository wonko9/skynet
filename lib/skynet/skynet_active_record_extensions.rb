class ActiveRecord::Base
  def send_later(method,opts=nil,save=nil)
    raise NoMethodError.new("Method: #{method} doesn't exist in #{self.class}") unless self.respond_to?(method)
    data = { 
      :model_class => self.class.to_s, 
      :model_id => self.id, 
      :method => method,       
    }
    data[:save] = 1 if save
    data[:opts] = opts.to_yaml if opts
    
    jobopts = {
      :single                => true,
      :map_tasks             => 1,
      :map_data              => [data],
      :name                  => "send_later #{self.class}##{method}",
      :map_name              => "",
      :map_timeout           => 1.hour,
      :reduce_timeout        => 1.hour,
      :master_timeout        => 8.hours,
      :master_result_timeout => 1.minute,
      :map_reduce_class      =>  Skynet::ActiveRecordAsync
    }   
    job = Skynet::AsyncJob.new(jobopts)
    job.run_master
  end
end

class <<ActiveRecord::Base
  def distributed_find(*args)
    all = ActiveRecord::Mapreduce.find(*args)
    all.model_class = self
    all
  end
end  

class Skynet::ActiveRecordAsync
  include SkynetDebugger
  
  def self.map(datas)
    datas.each do |data| 
      begin            
        model = data[:model_class].constantize.find(data[:model_id])
        if data[:opts]
          model.send(data[:method].to_sym, YAML.load(data[:opts]))
        else
          model.send(data[:method].to_sym)
        end
        model.save if data[:save]
      rescue Exception => e
        error "Error in #{self} #{e.inspect}"
      end
    end
    return
  end
end

module ActiveRecord  
  
  
  class Mapreduce
    BATCH_SIZE=1000 unless defined?(BATCH_SIZE)
    
    attr_accessor :find_args, :batch_size
    attr_reader :model_class
    
    def initialize(options = {})
      @find_args = options[:find_args]
      @batch_size = options[:batch_size] || BATCH_SIZE
      @model_class = options[:model_class]
    end

    def model_class=(model_c)
      @model_class = model_c.to_s
    end
    
    def self.find(*args)
      if not args.first.is_a?(Hash)
        args.shift
      end       
      if args.nil? or args.empty?
        args = {} 
      else
        args = *args
      end
      new(:find_args => args, :batch_size => args.delete(:batch_size), :model_class => args.delete(:model_class))
    end

    def each_range(opts={})
      opts = opts.clone
      opts[:id] || opts[:id] = 0
      rows = chunk_query(opts)            
      
      ii = 0                        
      if rows.empty?
        rows = [{"first" => 0, "last" => nil, "cnt" => ii}]
      end
      last_row = nil
      while rows.any?
        rows.each do |record| 
          last_row = record
          yield record, ii          
        end
        ii +=1                                                  
        return if last_row["last"].nil?
        rows = chunk_query(opts.merge(:id => rows.last["last"])) 
      end
      
      if last_row["last"] and (last_row["last"].to_i - last_row["first"].to_i) >= batch_size 
        catchall_row = {"first" => last_row["last"].to_i+1, "last" => nil, "cnt" => ii}
        yield catchall_row, ii
      end
    end  

    def chunk_query(opts={})
      mc = model_class.constantize
      table_name = mc.table_name

      conditions = "#{table_name}.id > #{opts[:id]} AND ((@t1:=(@t1+1) % #{batch_size})=0)"
      opts = opts.clone                                                               
      if opts[:conditions].nil? or opts[:conditions].empty?
        opts[:conditions] = conditions
      else
        opts[:conditions] += " AND " unless opts[:conditions].empty?
        opts[:conditions] += conditions
      end                
      limit = opts[:limit] ? "LIMIT #{opts[:limit]}" : nil
      # select @t2:=(@t2+1), @t3:=@t4, @t4:=id from profiles where ( ((@t1:=(@t1+1) % 1000)=0) or (((@t1+1) % 1000)=0) )  order by id LIMIT 100;

      # BEST
      # select @t1:=0, @t2:=0, @t3:=0, @t4:=0;
        # select @t2:=(@t2+1) as cnt, ((@t3:=@t4)+1) as first, @t4:=id as last from profiles where ((@t1:=(@t1+1) % 1000)=0) order by id LIMIT 100;
        # select (@t2:=(@t2+1) % 2) as evenodd, ((@t3:=@t4)+1) as first, @t4:=id as last from profiles where ((@t1:=(@t1+1) % 1000)=0) order by id LIMIT 100;

      mc.connection.execute('select @t1:=0, @t2:=-1, @t3:=0, @t4:=0')
      mc.connection.select_all("select @t2:=(@t2+1) as cnt, ((@t3:=@t4)+1) as first, @t4:=#{table_name}.id as last from #{table_name} #{opts[:joins]} where #{opts[:conditions]} ORDER BY #{table_name}.id #{limit}")

      # mc.connection.select_values(mc.send(:construct_finder_sql, :select => "#{mc.table_name}.id", :joins => opts[:joins], :conditions => conditions, :limit => opts[:limit], :order => :id))
    end
    
    
    def each(klass_or_method=nil,&block)    
      klass_or_method ||= model_class
      batches = []
      each_range(find_args) do |ids,ii|
        batch_count = ids["cnt"].to_i
        batches[batch_count] = [
                                  ids['first'].to_i, 
                                  ids['last'].to_i, 
                                  find_args.clone,
                                  model_class
                                ]
        if block_given?          
          batches[batch_count][4] = block
        else
          batches[batch_count][4] = "#{klass_or_method}" 
        end
      end
      
      job = nil
      jobopts = {
        :map_tasks => 20000,
        :map_data => batches,
        :name => "each #{model_class} MASTER",
        :map_name => "each #{model_class} MAP",
        :map_timeout      => 1.hour,
        :reduce_timeout   => 1.hour,
        :master_timeout => 8.hours,
        :master_result_timeout => 1.minute
        
      }   
      if block_given?
        job = Skynet::Job.new(jobopts.merge(:map_reduce_class => "#{self.class}"))
      else
        job = Skynet::AsyncJob.new(jobopts.merge(:map_reduce_class => "#{self.class}"))
      end         
      job.run
    end

    alias_method :mapreduce, :each

    def model_class
      @model_class || self.class.model_class
    end
    
    def self.model_class(model_class)
      (class << self; self; end).module_eval do
         define_method(:model_class) {model_class}
      end      
    end
    
    def self.log
      Skynet::Logger.get
    end

    def self.map(datas)
      datas.each do |data|    
        model_class = data[3].constantize
        table_name = model_class.table_name
        conditions = "#{table_name}.id >= #{data[0]}"
        conditions += " AND #{table_name}.id <= #{data[1]}" if data[1] > data[0]
        conditions = "(#{conditions})"
        # conditions = "ID BETWEEN #{data[0]} and #{data[1]}"
        if data[2].empty? or data[2][:conditions].empty?
          data[2] = {:conditions => conditions}
        else                      
          data[2][:conditions] += " AND #{conditions}"
        end                             
        data[2][:select] = "#{table_name}.*"
        model_class.find(:all, data[2]).each do |ar_object|
          begin
            if data[4].kind_of?(String)
              begin
                data[4].constantize.each(ar_object)
              rescue NameError
                if ar_object.respond_to?(data[4].to_sym)
                  ar_object.send(data[4])
                else
                  raise NameError.new("#{data[4]} is not a class or an instance method in #{model_class}")
                end
              end
            else
              data[4].call(ar_object)
            end
          rescue Exception => e
            if data[4].kind_of?(String)
              log.error("Error in #{data[4]} #{e.inspect} #{e.backtrace.join("\n")}")              
            else
              log.error("Error in #{self} with given block #{e.inspect} #{e.backtrace.join("\n")}")
            end
          end
        end
      end    
      nil
    end
  end
end
