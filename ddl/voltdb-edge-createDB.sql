
file voltdb-edge-removeDB.sql;
 
load classes ../jars/gson-2.8.1.jar;

load classes ../jars/voltdb-edge.jar;


file -inlinebatch END_OF_BATCH


CREATE TABLE utilities
(util_id bigint not null primary key
,util_name varchar(30) not null);

CREATE TABLE network_segments
(segment_id bigint not null primary key
,max_message_bytes_per_second bigint not null);

CREATE TABLE locations
(location_id bigint not null primary key
,segment_id bigint not null 
,location_name varchar(30) not null);

CREATE TABLE devices
(device_id bigint not null primary key
,model_number varchar(30) not null
,location_id bigint not null
,current_owner_id bigint not null
,last_firmware_update timestamp not null);

PARTITION TABLE devices ON COLUMN device_id;

CREATE INDEX devixes_idx1 ON devices (location_id);

CREATE TABLE models
(model_number varchar(30) not null primary key
,encoder_class_name varchar(512) not null);

CREATE TABLE device_messages
(device_id bigint not null 
,message_date timestamp not null
,message_id bigint not null
,internal_message_id bigint not null
,status_code varchar(5) not null
,segment_id bigint not null 
,current_owner_id bigint not null
,primary key (device_id,message_id));

PARTITION TABLE device_messages ON COLUMN device_id;

CREATE VIEW device_message_summary AS
SELECT status_code, count(*) how_many
FROM  device_messages
GROUP BY status_code;

CREATE VIEW device_message_activity AS
SELECT
truncate(MINUTE, message_date) message_date 
,status_code 
,segment_id  
,current_owner_id
, count(*) how_many
FROM device_messages
GROUP BY truncate(MINUTE, message_date) 
,status_code 
,segment_id  
,current_owner_id;

CREATE INDEX dma_ix1 ON device_message_activity(message_date);


CREATE STREAM segment_1_stream
PARTITION ON COLUMN device_id 
  EXPORT TO TOPIC segment_1_topic
  WITH KEY (message_id) VALUE (message_id,device_id,payload)
(message_id bigint not null
,device_id bigint not null 
,payload varchar(2048));

CREATE STREAM segment_0_stream
PARTITION ON COLUMN device_id 
  EXPORT TO TOPIC segment_0_topic
  WITH KEY (message_id) VALUE (message_id,device_id,payload)
(message_id bigint not null
,device_id bigint not null 
,payload varchar(2048));

CREATE STREAM powerco_1_stream
PARTITION ON COLUMN device_id 
  EXPORT TO TOPIC powerco_1_topic
  WITH KEY (message_id) VALUE (message_id,device_id,util_id,payload)
(message_id bigint not null
,device_id bigint not null 
,util_id bigint not null
,payload varchar(2048));

CREATE STREAM powerco_0_stream
PARTITION ON COLUMN device_id 
  EXPORT TO TOPIC powerco_0_topic
  WITH KEY (message_id) VALUE (message_id,device_id,util_id,payload)
(message_id bigint not null
,device_id bigint not null 
,util_id bigint not null
,payload varchar(2048));

CREATE STREAM error_stream
PARTITION ON COLUMN device_id 
  EXPORT TO TOPIC error_topic
  WITH KEY (message_id) VALUE (message_id,device_id,error_code,event_kind,payload)
(message_id bigint not null
,device_id bigint not null 
,error_code tinyint not null
,event_kind varchar(80)
,error_date timestamp default NOW
,payload varchar(2048));

CREATE VIEW error_summary_view AS
SELECT truncate(MINUTE, error_date) error_date
     , error_code
     , count(*) how_many
FROM error_stream
GROUP BY truncate(MINUTE, error_date) 
     , error_code;
     


 CREATE TABLE promBL_latency_stats
 (statname varchar(128) not null
 ,stathelp varchar(128) not null
 ,event_type varchar(128) not null
 ,event_name varchar(128) not null
 ,statvalue  bigint not null
 ,lastdate timestamp not null
 ,primary key (statname, event_type, event_name))
 USING TTL 1 MINUTES ON COLUMN lastdate;
 
 CREATE INDEX pls_ix1 ON promBL_latency_stats(lastdate);
 
 PARTITION TABLE promBL_latency_stats ON COLUMN statname;

 
CREATE PROCEDURE  
   PARTITION ON TABLE  devices COLUMN device_id
   FROM CLASS edgeprocs.ProvisionDevice;
   
CREATE PROCEDURE  
   PARTITION ON TABLE  devices COLUMN device_id
   FROM CLASS edgeprocs.SendMessageDownstream;
   
CREATE PROCEDURE  
   PARTITION ON TABLE  devices COLUMN device_id
   FROM CLASS edgeprocs.SendMessageUpstream;
   
CREATE PROCEDURE GetDevice
PARTITION ON TABLE DEVICES COLUMN DEVICE_ID
AS
BEGIN
SELECT *
FROM devices 
WHERE device_id = ?;
SELECT *
FROM device_messages 
WHERE device_id = ?
ORDER BY message_date, internal_message_id ;
END;

CREATE PROCEDURE GetDeviceMessage
PARTITION ON TABLE DEVICES COLUMN DEVICE_ID
AS
SELECT *
FROM device_messages 
WHERE device_id = ?
AND   message_id = ?
ORDER BY device_id,message_id ;

CREATE PROCEDURE GetDevicesForLocation
AS
SELECT d.device_id, m.encoder_class_name, m.model_number 
FROM   devices d   
   ,   locations l
   ,   models m
WHERE d.location_id = l.location_id
AND   l.segment_id = ?
AND   d.model_number = m.model_number
ORDER BY d.device_id;


CREATE PROCEDURE GetDevicesForPowerco
AS
SELECT d.device_id, m.encoder_class_name, m.model_number 
FROM   devices d   
   ,   locations l
   ,   models m
WHERE d.location_id = l.location_id
AND   d.current_owner_id = ?
AND   d.model_number = m.model_number
ORDER BY d.device_id;

CREATE PROCEDURE GetStats__promBL AS
BEGIN
--
select  statname, stathelp, event_type, event_name, statvalue 
from promBL_latency_stats
order by statname, stathelp, event_type, event_name, statvalue ;
--
select  'bl_transaction_status' statname
, 'bl_transaction_status' stathelp 
,status_code  
,  how_many  statvalue from device_message_summary;
--
select 'bl_message_activity' statname
, 'bl_tmessage_activity' stathelp 
,status_code 
,segment_id  
,current_owner_id
, how_many statvalue
from device_message_activity 
where message_date = dateadd(minute,-1,truncate(minute,now));
END;

END_OF_BATCH

file voltdb-edge-testdata.sql;
