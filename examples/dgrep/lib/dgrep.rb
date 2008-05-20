class Dgrep
  include SkynetDebugger

  # Takes an array that looks like [ [filename1,["word1","word2"]], [filename2,["word1","word2"]] ]
  # Returns an array that looks like [ [word1, cnt], [word2,cnt], [word1, cnt] ]
  # Map gets an array and should return an array
  def self.map(map_datas)
    results = []
    map_datas.each do |filename,words|
      next unless File.file?(filename)
      words.each do |word|
        cnt = File.read(filename).scan(/#{word}/i).size
        results << [word,cnt] if cnt and cnt > 0
      end
    end
    results
  end

  # The chosen reduce partiioner attempts group keys by partition.  For example, if you choose 2 words and 2 reducers it will make sure
  # the first word goes to the first reducer and the second word to the second reducer.  If you only have 1 reducer (the defaukt)
  # It doesn't bother partitioning.  You can write as complex a partiioner as you'd like.
  def self.reduce_partition(post_map_data,new_partitions)
    Skynet::Partitioners::ArrayDataSplitByFirstEntry.reduce_partition(post_map_data,new_partitions)

    ## I've included the contents of the above method so you can see how to write a reduce_partitioner
    # partitions = []
    # (0..new_partitions - 1).each { |i| partitions[i] = Array.new }
    # keys_seen = {}
    # post_map_data.each do |partition|
    #   partition.each do |array|
    #     next unless array.is_a?(Array) and array.size >= 2
    #     if array[0].kind_of?(Fixnum)
    #       key = array[0]
    #     else
    #       keys_seen[array[0]] ||= keys_seen.keys.size
    #       key = keys_seen[array[0]]
    #     end
    #     partitions[key % new_partitions] << array
    #   end
    # end
    # partitions

  end

  # Takes an array that looks like  [ [word1, cnt], [word2,cnt], [word1, cnt] ]
  # reduce also gets an array
  def self.reduce(reduce_datas)
    results = {}
    reduce_datas.each do |reduce_data|
      results[reduce_data[0]] ||= 0
      results[reduce_data[0]] += reduce_data[1]
    end
    results
  end

end