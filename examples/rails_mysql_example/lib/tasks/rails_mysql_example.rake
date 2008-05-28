namespace :db do
  desc "Initialize Defaults"
  task :initialize_defaults => :environment do
    puts "Clearing User and UserFavorite tables"
    User.destroy_all
    puts "Creating Someguy"
    User.create(:name => "Someguy", :favorites => "boats,fishing,boat-fishing", :email => 'skynet@mailinator.com')
    puts "Creating AnotherGuy"
    User.create(:name => "AnotherGuy", :favorites => "eating,sleeping,sleep-walking", :email => 'skynet@mailinator.com')
    puts "Creating YoMamma"
    User.create(:name => "YoMomma", :favorites => "kidding,joking,pining", :email => 'skynet@mailinator.com')
    puts "finished"
  end

  desc "Migrate Favorites"
  task :migrate_favorites => :environment do
    User.distributed_find(:all).mapreduce(:migrate_favorites)
  end

end
