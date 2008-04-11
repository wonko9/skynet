class Skynet
  class MapreduceTest
    include SkynetDebugger
            
    def self.map(datas)
      results = {}
      datas.each do |data|
        results[data] ||= 0
        results[data] += 1
      end                 
      [results]      
    end
    
    def self.reduce(datas)
      results = {}
      datas.each do |hashes|
        hashes.each do |key,value|
          results[key] ||= 0
          results[key] += value
        end
      end
      results
    end
  end

  class ProfileCountTest
    def self.run
      job = Skynet::Job.new(
        :mappers          => 2,
        :reducers         => 1,
        :map_reduce_class => self,
        :map_data         => [OpenStruct.new({:created_by => 2}),OpenStruct.new({:created_by => 2}),OpenStruct.new({:created_by => 3})]
      )
      results = job.run
    end

    def self.map(profiles)
      result = Array.new
      profiles.each do |profile|
        result << [profile.created_by, 1] if profile.created_by
      end
      result
    end

    def self.reduce(pairs)
      totals = Hash.new
      pairs.each do |pair|
        created_by, count = pair[0], pair[1]
        totals[created_by] ||= 0
        totals[created_by] += count
      end
      totals
    end
  end
  
end
