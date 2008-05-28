CREATE TABLE skynet_message_queues (
  id int(11) NOT NULL auto_increment,
  queue_id int(11) default '0',
  tran_id bigint(20) unsigned default NULL,
  created_on datetime default NULL,
  updated_on datetime default NULL,
  tasktype varchar(255) default NULL,
  task_id bigint(20) unsigned default NULL,
  job_id bigint(20) unsigned default NULL,
  raw_payload longtext,
  payload_type varchar(255) default NULL,
  name varchar(255) default NULL,
  expiry int(11) default NULL,
  expire_time decimal(16,4) default NULL,
  iteration int(11) default NULL,
  version int(11) default NULL,
  timeout decimal(16,4) default NULL,
  retry int(11) default '0',
  PRIMARY KEY  (id),
  UNIQUE KEY index_skynet_message_queues_on_tran_id (tran_id),
  KEY index_skynet_message_queues_on_job_id (job_id),
  KEY index_skynet_message_queues_on_task_id (task_id),
  KEY index_skynet_mqueue_for_take (queue_id,tasktype,payload_type,expire_time)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE skynet_queue_temperature (
  id int(11) NOT NULL auto_increment,
  queue_id int(11) default '0',
  updated_on datetime default NULL,
  count int(11) default '0',
  temperature decimal(6,4) default NULL,
  type varchar(255) default NULL,
  PRIMARY KEY  (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;