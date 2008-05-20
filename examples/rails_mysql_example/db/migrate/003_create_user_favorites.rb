class CreateUserFavorites < ActiveRecord::Migration
  def self.up
    create_table :user_favorites do |t|
      t.integer :user_id
      t.string :favorite

      t.timestamps
    end
  end

  def self.down
    drop_table :user_favorites
  end
end
