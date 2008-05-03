require File.dirname(__FILE__) + '/test_helper.rb'

class AllModelsTest < Test::Unit::TestCase
  
  def test_true
    true
  end

  # def test_map
  #   eachmeth = lambda do |profile|
  #     profile.claimed = false
  #     profile.save        
  #   end
  #   data = [1,10,{:conditions => "profiles.claimed = 1"},"Profile",eachmeth]
  #   
  #   assert Profile.find(2).claimed, "claimed"
  # 
  #   ActiveRecord::Mapreduce.map([data])
  #   
  #   assert !Profile.find(2).claimed, "not claimed"
  # end
  # 
  # def test_each_with_proc
  #   Skynet.solo do
  #     ActiveRecord::Mapreduce.find(:all, :conditions => "claimed = 1", :batch_size => 2, :model_class => "Profile", :limit => 5).each do |profile|
  #       profile.suffix = 'z'
  #       profile.save        
  #     end
  #   end      
  #   assert_equal 'z', Profile.find(2).suffix
  # end
  # 
  # def test_big_batch
  #   Skynet.solo do
  #     ActiveRecord::Mapreduce.find(:all, :conditions => "claimed = 1", :batch_size => 200, :model_class => "Profile").each do |profile|
  #       profile.suffix = 'z'
  #       profile.save        
  #     end
  #   end      
  #   assert_equal 'z', Profile.find(2).suffix
  # end
  # 
  # def test_joins
  #   p = Profile.find(1)
  #   p.details.zodiac_sign = 'hermit'
  #   p.details.save
  #   Skynet.solo do
  #     ActiveRecord::Mapreduce.find(:all, :conditions => "profile_details.zodiac_sign='hermit'", :joins => "JOIN profile_details ON profiles.id = profile_details.profile_id", :batch_size => 2, :model_class => "Profile").each do |profile|
  #       profile.suffix = 'z'
  #       profile.save        
  #     end
  #   end      
  #   assert_equal 'z', Profile.find(1).suffix
  # end
  # 
  # def test_stragglers
  #   profiles = Profile.find(:all)
  #   Skynet.solo do
  #     ActiveRecord::Mapreduce.find(:all, :batch_size => profiles.size-1, :model_class => "Profile").each do |profile|
  #       profile.suffix = 'z'
  #       profile.save        
  #     end
  #   end                      
  #   profiles.last.reload
  #   assert_equal 'z', profiles.last.suffix      
  # end
  # 
  # def test_small_limit
  #   Skynet.solo do
  #     ActiveRecord::Mapreduce.find(:all, :conditions => "claimed = 1", :batch_size => 2, :model_class => "Profile", :limit => 3).each do |profile|
  #       profile.suffix = 'z'
  #       profile.save        
  #     end
  #   end      
  #   assert_equal 'z', Profile.find(2).suffix
  # end
  # 
  # def test_each_with_proc_exception
  #   Skynet.solo(:SKYNET_LOG_LEVEL => Logger::FATAL) do
  #     ActiveRecord::Mapreduce.find(:all, :conditions => "claimed = 1", :batch_size => 2, :model_class => "Profile").each do |profile|
  #       raise "BUSTED" if profile.id == 6
  #       profile.suffix = 'z'
  #       profile.save        
  #     end
  #   end      
  #   assert_equal 'z', Profile.find(2).suffix
  #   assert_equal nil, Profile.find(6).suffix
  # end
  # 
  # def test_each_with_class_exception
  #   Skynet.solo(:SKYNET_LOG_LEVEL => Logger::FATAL) do
  #     ActiveRecord::Mapreduce.find(:all, :conditions => "claimed = 1", :batch_size => 2, :model_class => "Profile").each(ActiveRecord::MapreduceExTest)
  #   end      
  #   assert_equal 'k', Profile.find(2).suffix
  #   assert_equal nil, Profile.find(8).suffix
  # end
  # 
  # def test_each_with_class
  #   Skynet.solo do
  #     ActiveRecord::Mapreduce.find(:all, :conditions => "claimed = 1", :batch_size => 2, :model_class => "Profile").each(self.class)
  #   end      
  #   assert_equal 'k', Profile.find(2).suffix
  # end
  # 
  # def test_distributed_each
  #   Skynet.solo do
  #     Profile.distributed_find(:all, :conditions => "claimed = 1", :batch_size => 2, :model_class => "Profile").each do |profile|
  #         profile.suffix = 'gg'
  #         profile.save        
  #     end
  #     assert_equal 'gg', Profile.find(2).suffix
  #     Profile.distributed_find(:all, :conditions => "claimed = 1", :batch_size => 2, :model_class => "Profile").each(self.class)
  #     assert_equal 'k', Profile.find(2).suffix
  #   end
  # end
  # 
  # def test_distributed_each_with_symbol
  #   Skynet.solo do
  #     Profile.distributed_find(:all, :conditions => "claimed = 1", :batch_size => 2, :model_class => "Profile").each(:mark_modified)
  #   end
  #   assert ModifiedProfile.find(:first, :conditions => "profile_id = 2")
  # end
  #     
  # def self.each(profile)
  #   profile.suffix = 'k'
  #   profile.save        
  # end

end

class AllModelsExTest
  def self.each(profile)
    raise "BUSTED" if profile.id == 8
    profile.suffix = 'k'
    profile.save        
  end
end

