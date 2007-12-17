module Enumerable    
  def mapreduce(klass=nil,options={},&block)               
    data = []
    if self.is_a?(Hash)
      self.each {|k,v| data << {k => v}}
    else
      data = self
    end
    jobopts = {
      :map_tasks              => 20000,
      :map_data               => data,
      :name                   => "#{klass} Enumerable MASTER",
      :map_name               => "#{klass} Enumerable MAP",
      :reduce_name            => "#{klass} Enumerable REDUCE",
      :map_timeout            => 3600,
      :reduce_timeout         => 3600,
      :master_timeout         => 3600,
      :master_result_timeout  => 3600,
      :async                  => true
    }                                

    jobopts[:map_reduce_class] = klass.to_s if klass

    options.each { |k,v| jobopts[k] = v }
    if block_given?
      jobopts[:map] = block
    end                               
    
    if block_given? or not jobopts[:async]
      job = Skynet::Job.new(jobopts)
    else
      job = Skynet::AsyncJob.new(jobopts)
    end
    job.run    
  end  
end