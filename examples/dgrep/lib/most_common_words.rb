class IO
  def each_word(&b)
    each do |line|
     line.scan(/\w+/, &b)
    end
  end
end

class MostCommonWords
  include SkynetDebugger

  def self.map(map_datas)
    results = Hash.new(0)
    map_datas.each do |filename|
      begin
        File.open(filename).each_word do |word|
          next if word.length <= 5
          results[word.downcase] += 1
        end
      rescue Errno::EISDIR
        # skip directories
      end
    end
    results.sort{|a,b| a[1]<=>b[1]}.reverse.first(100)
  end

  # The chosen reduce partiioner attempts group keys by partition.  For example, if you choose 2 words and 2 reducers it will make sure
  # the first word goes to the first reducer and the second word to the second reducer.  If you only have 1 reducer (the defaukt)
  # It doesn't bother partitioning.  You can write as complex a partiioner as you'd like.
  def self.reduce_partition(post_map_data,new_partitions)
    Skynet::Partitioners::ArrayDataSplitByFirstEntry.reduce_partition(post_map_data,new_partitions)
  end

  # Takes an array that looks like  [ [word1, cnt], [word2,cnt], [word1, cnt] ]
  # reduce also gets an array
  def self.reduce(reduce_datas)
    results = Hash.new(0)
    reduce_datas.each do |reduce_data|
      results[reduce_data[0]] += reduce_data[1]
    end
    results.sort{|a,b| a[1]<=>b[1]}.reverse.first(10).sort
  end

end

