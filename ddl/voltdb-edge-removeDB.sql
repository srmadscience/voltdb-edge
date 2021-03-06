
DROP TASK MarkMessagesStaleTask IF EXISTS;

DROP PROCEDURE MarkMessagesStale IF EXISTS;
DROP PROCEDURE ProvisionDevice IF EXISTS;
DROP PROCEDURE SendMessageDownstream IF EXISTS;
DROP PROCEDURE SendMessageUpstream IF EXISTS;
DROP PROCEDURE GetDevice IF EXISTS;
DROP PROCEDURE GetDeviceMessage IF EXISTS;
DROP PROCEDURE GetDevicesForLocation IF EXISTS;
DROP PROCEDURE GetDevicesForLocationTotal IF EXISTS;
DROP PROCEDURE GetDevicesForPowerco IF EXISTS;
DROP PROCEDURE GetDevicesForPowercoTotal IF EXISTS;
DROP PROCEDURE GetStats__promBL IF EXISTS;

DROP VIEW error_summary_view IF EXISTS;
DROP VIEW device_summary IF EXISTS;
DROP VIEW device_message_summary IF EXISTS;
DROP VIEW device_message_activity IF EXISTS;

DROP TABLE utilities IF EXISTS;
DROP TABLE device_messages IF EXISTS;
DROP TABLE promBL_latency_stats IF EXISTS;
DROP TABLE network_segments IF EXISTS;
DROP TABLE locations IF EXISTS;
DROP TABLE devices IF EXISTS;
DROP TABLE models IF EXISTS;

drop stream segment_0_stream IF EXISTS;
drop stream segment_1_stream IF EXISTS;
drop stream segment_2_stream IF EXISTS;
drop stream segment_3_stream IF EXISTS;
drop stream segment_4_stream IF EXISTS;
drop stream segment_5_stream IF EXISTS;
drop stream segment_6_stream IF EXISTS;
drop stream segment_7_stream IF EXISTS;
drop stream segment_8_stream IF EXISTS;
drop stream segment_9_stream IF EXISTS;
drop stream powerco_0_stream IF EXISTS;
drop stream powerco_1_stream IF EXISTS;
drop stream error_stream IF EXISTS;

