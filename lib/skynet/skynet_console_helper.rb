
def mq
  @mq ||= Skynet::MessageQueue.new
end                       

def status
  mq.status
end

def increment_worker_version
  mq.increment_worker_version
end

def get_worker_version
  mq.get_worker_version
end

def set_worker_version(*args)
  mq.set_worker_version(*args)
end
