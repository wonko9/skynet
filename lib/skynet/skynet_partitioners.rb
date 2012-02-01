class Skynet
                                       # Collection of partitioning utilities
  class Partitioners
    include SkynetDebugger

    # Split one block of data into partitions
    #
    class SimplePartitionData < Partitioners

      def self.reduce_partition(data, partitions)
        partitioned_data = Array.new

        # If data size is significantly greater than the number of desired
        # partitions, we can divide the data roughly but the last partition
        # may be smaller than the others.
        #
        return data if (not data) or data.empty?

        if partitions >= data.length
          data.each do |datum|
           partitioned_data << [datum]
          end
        elsif (data.length >= partitions * 2)
          # Use quicker but less "fair" method
          size = data.length / partitions

          if (data.length % partitions != 0)
            size += 1 # Last slice of leftovers
          end

          (0..partitions - 1).each do |i|
            partitioned_data[i] = data[i * size, size]
          end
        else
          # Slower method, but partitions evenly
          partitions = (data.size < partitions ? data.size : partitions)
          (0..partitions - 1).each { |i| partitioned_data[i] = Array.new }

          data.each_with_index do |datum, i|
            partitioned_data[i % partitions] << datum
          end
        end

        partitioned_data
      end
    end


    class RecombineAndSplit < Partitioners
      # Tries to be smart about what kind of data its getting, whether array of arrays or array of arrays of arrays.
      #
      def self.reduce_partition(post_map_data,new_partitions)
        return post_map_data unless post_map_data.is_a?(Array) and (not post_map_data.empty?) and post_map_data.first.is_a?(Array)
        ### Why did I do this?  It breaks badly.
        # if not post_map_data.first.first.is_a?(Array)
        #   partitioned_data = post_map_data.flatten
        # else
          partitioned_data = post_map_data.inject(Array.new) do |data,part|
            data += part
          end
        # end
        partitioned_data = Skynet::Partitioners::SimplePartitionData.reduce_partition(partitioned_data, new_partitions)
        debug "POST PARTITIONED DATA_SIZE", partitioned_data.size
        debug "POST PARTITIONED DATA", partitioned_data
        partitioned_data
      end
    end

    class ArrayDataSplitByFirstEntry < Partitioners
      # Smarter partitioner for array data, generates simple sum of array[0]
      # and ensures that all arrays sharing that key go into the same partition.
      #
      def self.reduce_partition(post_map_data, new_partitions)
        partitions = []
        (0..new_partitions - 1).each { |i| partitions[i] = Array.new }
        cnt = 0
        post_map_data.each do |partition|
          partition.each do |array|
            next unless array.is_a?(Array) and array.size >= 2
            if array[0].kind_of?(Fixnum)
              key = array[0] % new_partitions
            elsif array[0].kind_of?(String)
              key = array[0].sum % new_partitions
            else
              cnt += 1
              key = cnt % new_partitions
            end
            partitions[key] << array
          end
        end
        partitions
      end
    end

  end
end
