module Enumerable    
  def mapreduce(klass=nil,options={},&block)               
    data = []
    if self.is_a?(Hash)
      self.each {|k,v| data << {k => v}}
    else
      data = self
    end
    jobopts = {
      :mappers                => 20000,
      :map_data               => data,
      :name                   => "#{klass} Enumerable MASTER",
      :map_name               => "#{klass} Enumerable MAP",
      :reduce_name            => "#{klass} Enumerable REDUCE",
      :map_timeout            => 3600,
      :reduce_timeout         => 3600,
      :master_timeout         => 3600,
      :master_result_timeout  => 3600
    }                                

    jobopts[:map_reduce_class] = klass.to_s if klass

    options.each { |k,v| jobopts[k] = v }
    if block_given?
      jobopts[:map] = block
    end                               
    
    if block_given? or not jobopts[:async]
      job = Skynet::Job.new(jobopts.merge(:local_master => true))
    else
      job = Skynet::AsyncJob.new(jobopts)
    end
    job.run    
  end  
end

class String
  ### THIS IS TAKEN DIRECTLY FROM ActiveSupport::Inflector
  # Constantize tries to find a declared constant with the name specified
  # in the string. It raises a NameError when the name is not in CamelCase
  # or is not initialized.
  #
  # Examples
  #   "Module".constantize #=> Module
  #   "Class".constantize #=> Class
  def constantize
    unless /\A(?:::)?([A-Z]\w*(?:::[A-Z]\w*)*)\z/ =~ self
      raise NameError, "#{camel_cased_word.inspect} is not a valid constant name!"
    end

    Object.module_eval("::#{$1}", __FILE__, __LINE__)
  end
end