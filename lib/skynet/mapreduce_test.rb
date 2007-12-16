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
end
