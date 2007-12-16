module Enumerable    
  def mapreduce(klass)               
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
      :map_name               => "#{klass} Enumerable REDUCE",
      :map_timeout            => 3600,
      :reduce_timeout         => 3600,
      :master_timeout         => 3600,
      :master_result_timeout  => 3600,
      :map_reduce_class       => klass.to_s
      
    }   
    job = Skynet::AsyncJob.new(jobopts)
    job.run    
  end  
end