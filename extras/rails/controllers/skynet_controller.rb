class Admin::SkynetController < AdminController

  def index
    begin
      setup
      if params[:skynet_message_queue]
        Skynet.configure(:MYSQL_MESSAGE_QUEUE_TABLE => params[:skynet_message_queue]) do
          @stats = @mq.stats
        end
      else
        @stats = @mq.stats
      end
      @stats.merge!(Skynet::Manager.stats_for_hosts)
      @stats[:hosts] = @stats[:servers].size
    rescue Exception => e
      logger.error "ERROR #{e.inspect} #{e.backtrace.join("\n")}"      
    end
  end
  
  # plain text page that will be used by monitoring scripts
  def status
    begin
      setup
      stats = @mq.stats
      stats[:servers] = stats[:servers].keys.join(",")
      stats.each { |k,v| stats[k.to_s] = stats.delete(k) }
      text  = stats.keys.sort.collect{ |k| "#{k}:#{stats[k]}" }.join("\n") + "\n"
      render :text => text, :content_type => 'text/plain'
    rescue Exception => e
      render :text => "skynet is down\n", :content_type => 'text/plain'
    end
  end  
  
  private
  
  def setup
    @mq  ||= Skynet::MessageQueue.new(Skynet::CONFIG[:MESSAGE_QUEUE_ADAPTER])
    @last_updated = Time.now.strftime('%r')
  end
  
end
