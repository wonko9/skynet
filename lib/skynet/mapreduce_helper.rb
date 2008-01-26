module MapreduceHelper
# You can include the MapreduceHelper into your class to give you standard self.map and self.reduce methods.
# You need only implement self.map_each and self.reduce_each methods which accept a single item (istead of an arrad)
#
# Example Usage:
# This example is a bit contrived.
# 
#   class MapReduceTest
#     include MapreduceHelper
#   
#     def self.run
#       job = Skynet::Job.new(
#         :mappers          => 2, 
#         :reducers         => 1,
#         :map_reduce_class => self,
#         :map_data         => ['http://www.geni.com'.'http://www.yahoo.com','http://www.cnet.com']
#       )    
#       results = job.run
#     end
#   
#     def self.map_each(url)
#       SomeUrlSlurper.gather_results(url)  # returns an array of urls of sites that link to the given url
#     end
#   
#     def self.reduce(linked_from_url)
#       SomeUrlSluper.find_text("mysite", linked_from_url)   # finds all the times "mysite" appears in the given url, which we know links to the url given in the map_data
#     end
#   end
# 
#   MapReduceTest.run

  
  def self.included(base)
    base.extend MapreduceHelper
  end               

  # Takes an array of map_data, iterates over that array calling self.map_each(item) for each 
  # item in that array.   Catches exceptions in each iteration and continues processing.
  def map(map_data_array)
    raise Skynet::Job::BadMapOrReduceError.new("#{self.class} has no self.map_each method.") unless self.respond_to?(:map_each)
    if map_data_array.is_a?(Array)
      results = []
      map_data_array.each do |data|
        begin
          results << map_each(data)
        rescue Exception => e
          error "ERROR IN #{self} [#{e.class} #{e.message}] #{e.backtrace.join("\n")}"
        end                
      end
      results
    else
      map_each(map_data_array)
    end
  end
  
  # Takes an array of post reduce_partitioned data, iterates over that array calling self.reduce_each(item) for each 
  # item in that array.   Catches exceptions in each iteration and continues processing.
  def reduce(reduce_partitioned_data_array)
    raise Skynet::Job::BadMapOrReduceError.new("#{self.class} has no self.reduce_each method.") unless self.respond_to?(:reduce_each)
    if reduce_partitioned_data_array.is_a?(Array)
      results = []
      reduce_partitioned_data_array.each do |data|
        begin
          results << reduce_each(data)
        rescue Exception => e
          error "ERROR IN #{self} [#{e.class} #{e.message}] #{e.backtrace.join("\n")}"
        end        
      end
      results
    else
      reduce_each(reduce_partitioned_data_array)
    end
  end  
end