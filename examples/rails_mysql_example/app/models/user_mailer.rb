class UserMailer < ActionMailer::Base  
  class Error < StandardError; end

  def welcome(user)
    @recipients   = "#{user.email}"
    @from         = "Mycompany <root@localhost>"
    @sent_on      = Time.now
    @subject      = 'Your account has been activated!'
    @body[:url]   = "http://localhost:3000/"
    @body[:user]  = user
  end
end