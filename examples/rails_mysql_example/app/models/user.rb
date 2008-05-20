class User < ActiveRecord::Base
  
  has_many :user_favorites, :dependent => :destroy
  
  def migrate_favorites
    return unless self.favorites
    self.favorites.split(",").each do |favorite|
      self.user_favorites.create(:favorite => favorite)
    end
    self.favorites = nil
    self.save
  end
  
  def send_welcome_email
    sleep 5
    UserMailer.deliver_welcome(self)
  end
end
                       