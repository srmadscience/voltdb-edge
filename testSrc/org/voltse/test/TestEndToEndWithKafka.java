package org.voltse.test;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.topics.VoltDBKafkaPartitioner;
import org.voltse.edge.edgeencoders.JsonEncoderImpl;
import org.voltse.edge.edgeencoders.ModelEncoderIFace;
import org.voltse.edge.edgemessages.EnableFeatureMessage;
import org.voltse.edge.edgemessages.MessageIFace;

import com.google.gson.Gson;

import edgeprocs.ReferenceData;

class TestEndToEndWithKafka {

    private static final String SEGMENT_1_TOPIC = "segment_1_topic";
    private static final String UPSTREAM_TOPIC = "upstream_1_topic";
    private static final String DOWNSTREAM_TOPIC = "downstream_1_topic";
    private static final String POWERCO_1_TOPIC = "powerco_1_topic";

    final long startMs = System.currentTimeMillis();

    Consumer<Long, String> kafkaDeviceConsumer;
    Consumer<Long, String> kafkaPowercoConsumer;
    Producer<Long, String> kafkaProducer;
    Client c;
    Gson g = new Gson();

    String[] tablesToDelete = { "DEVICES", "device_messages" };

    ModelEncoderIFace jsonEncoder = new JsonEncoderImpl();

    int nextDeviceId = 100;

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
    }

    @BeforeEach
    void setUp() throws Exception {

        c = connectVoltDB("localhost");

        for (String element : tablesToDelete) {
            c.callProcedure("@AdHoc", "DELETE FROM " + element + ";");
        }

        try {
            kafkaDeviceConsumer = connectToKafkaConsumer("localhost",
                    "org.apache.kafka.common.serialization.LongDeserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");

            kafkaDeviceConsumer.subscribe(Collections.singletonList(SEGMENT_1_TOPIC));

            kafkaPowercoConsumer = connectToKafkaConsumer("localhost",
                    "org.apache.kafka.common.serialization.LongDeserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");

            kafkaPowercoConsumer.subscribe(Collections.singletonList(POWERCO_1_TOPIC));

        } catch (Exception e) {
            msg(e.getMessage());
            throw (e);
        }

        try {
            kafkaProducer = connectToKafkaProducer("localhost", "org.apache.kafka.common.serialization.LongSerializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
        } catch (Exception e) {
            msg(e.getMessage());
            throw (e);
        }

    }

    @AfterEach
    void tearDown() throws Exception {

        c.drain();
        c.close();
        c = null;

        kafkaDeviceConsumer.close();
        kafkaDeviceConsumer = null;
        kafkaPowercoConsumer.close();
        kafkaPowercoConsumer = null;
        kafkaProducer.close();
        kafkaProducer = null;
    }

    @Test
    void testWithNoPowerco() {

        final long startMs = System.currentTimeMillis();

        final long recordId = System.currentTimeMillis();

        try {

            // Create a generic meter
            long deviceId = testProvison(TestSendDownstreamWithVolt.TEST_OWNER);
            long externallMessageId = recordId;
            long latencyMs = -1;
            String errorMessage = null;
            Date createDate = null;
            int destinationSegmentId = -1;
            String featureName = "NIGHTSETTING";
            boolean enabled = true;

            EnableFeatureMessage originalMessage = new EnableFeatureMessage(deviceId, externallMessageId, latencyMs,
                    errorMessage, createDate, destinationSegmentId, featureName, enabled, 1);

            String serializedMessage = Base64.getEncoder()
                    .encodeToString(jsonEncoder.encode(originalMessage).getBytes());

            ClientResponse cr = c.callProcedure("SendMessageDownstream", originalMessage.getDeviceId(),
                    TestSendDownstreamWithVolt.TEST_OWNER, serializedMessage);

            checkResponseOK(cr);

            EnableFeatureMessage recoveredMessage = (EnableFeatureMessage) receiveJsonDeviceMessage(SEGMENT_1_TOPIC,
                    originalMessage.getExternallMessageId());

            if (recoveredMessage.deviceId != originalMessage.deviceId) {
                fail("Device id mismatch");
            }

            if (recoveredMessage.latencyMs != -1) {
                fail("latencyMs set");
            }

            latencyMs = System.currentTimeMillis() - startMs;

            if (recoveredMessage.errorMessage != null) {
                fail("errorMessage set");
            }

            if (recoveredMessage.destinationSegmentId == -1) {
                fail("destinationSegmentId id mismatch");
            }

            if (!recoveredMessage.featureName.equals(featureName)) {
                fail("featureName mismatch");
            }

            if (!recoveredMessage.isEnabled()) {
                fail("isEnabled mismatch");
            }

            sendJsonMessageUpstream(UPSTREAM_TOPIC, recoveredMessage);

            cr = c.callProcedure("GetDeviceMessage", originalMessage.getDeviceId(),
                    originalMessage.getExternallMessageId());

            cr.getResults()[0].advanceRow();
            String statusCode = cr.getResults()[0].getString("STATUS_CODE");

            if (!statusCode.equals(ReferenceData.MESSAGE_DONE + "")) {
                fail("Expected " + ReferenceData.MESSAGE_DONE + ", got " + statusCode);

            }

        } catch (Exception e) {
            fail(e);
        }

    }

    @Test
    void testWithPowerco() {

        final long startMs = System.currentTimeMillis();

        final long recordId = System.currentTimeMillis();

        try {

            // Create a generic meter
            long deviceId = testProvison(TestSendDownstreamWithVolt.TEST_OWNER);
            long externallMessageId = recordId;
            long latencyMs = -1;
            String errorMessage = null;
            Date createDate = null;
            int destinationSegmentId = -1;
            String featureName = "NIGHTSETTING";
            boolean enabled = true;

            EnableFeatureMessage originalMessage = new EnableFeatureMessage(deviceId, externallMessageId, latencyMs,
                    errorMessage, createDate, destinationSegmentId, featureName, enabled, 1);

            sendJsonMessageDownstream(DOWNSTREAM_TOPIC, TestSendDownstreamWithVolt.TEST_OWNER, originalMessage);

            EnableFeatureMessage recoveredMessage = (EnableFeatureMessage) receiveJsonDeviceMessage(SEGMENT_1_TOPIC,
                    originalMessage.getExternallMessageId());

            if (recoveredMessage.deviceId != originalMessage.deviceId) {
                fail("Device id mismatch");
            }

            if (recoveredMessage.latencyMs != -1) {
                fail("latencyMs set");
            }

            latencyMs = System.currentTimeMillis() - startMs;

            if (recoveredMessage.errorMessage != null) {
                fail("errorMessage set");
            }

            if (recoveredMessage.destinationSegmentId == -1) {
                fail("destinationSegmentId id mismatch");
            }

            if (!recoveredMessage.featureName.equals(featureName)) {
                fail("featureName mismatch");
            }

            if (!recoveredMessage.isEnabled()) {
                fail("isEnabled mismatch");
            }

            sendJsonMessageUpstream(UPSTREAM_TOPIC, recoveredMessage);

            // TODO
//            EnableFeatureMessage endStateMessage = (EnableFeatureMessage)  receiveJsonPowercoMessage(POWERCO_1_TOPIC,
//                    originalMessage.getExternallMessageId());
//

            ClientResponse cr = c.callProcedure("GetDeviceMessage", originalMessage.getDeviceId(),
                    originalMessage.getExternallMessageId());

            cr.getResults()[0].advanceRow();
            String statusCode = cr.getResults()[0].getString("STATUS_CODE");

            if (!statusCode.equals(ReferenceData.MESSAGE_DONE + "")) {
                fail("Expected " + ReferenceData.MESSAGE_DONE + ", got " + statusCode);

            }

        } catch (Exception e) {
            fail(e);
        }

    }

    private void checkResponseOK(ClientResponse cr) {
        if (cr.getStatus() != ClientResponse.SUCCESS) {
            fail(cr.getStatusString());
        }

        if (cr.getAppStatus() != ReferenceData.OK) {
            fail(cr.getAppStatusString());
        }
    }

    private void sendJsonMessageDownstream(String topicname, long testOwner, MessageIFace message) throws Exception {

        String encodedMessage = jsonEncoder.encode(message);

        String payload = message.getDeviceId() + "," + testOwner + ","
                + Base64.getEncoder().encodeToString(encodedMessage.getBytes());
        final ProducerRecord<Long, String> record = new ProducerRecord<>(topicname, message.getDeviceId(), payload);

        kafkaProducer.send(record).get();

    }

    private void sendJsonMessageUpstream(String topicname, MessageIFace message) throws Exception {

        try {
            String encodedMessage = jsonEncoder.encode(message);

            String payload = message.getDeviceId() + ","
                    + Base64.getEncoder().encodeToString(encodedMessage.getBytes());
            final ProducerRecord<Long, String> record = new ProducerRecord<>(topicname, message.getDeviceId(), payload);

            kafkaProducer.send(record).get();

        } catch (Exception e) {
            fail("sendJsonMessageUpstream:" + e.getMessage());
        }

    }

    private MessageIFace receiveJsonDeviceMessage(String topic, long externalMessageId) throws Exception {

        ConsumerRecord<Long, String> ourRecord = getNextDeviceRecord(topic);

        if (ourRecord == null) {
            fail("receiveJsonMessage == null");
        }

        String[] recordAsCSV = ourRecord.value().split(",");

        recordAsCSV[2] = new String(Base64.getDecoder().decode(recordAsCSV[2].getBytes()));

        MessageIFace record = jsonEncoder.decode(recordAsCSV[2]);

        if (ourRecord.key() != record.getExternallMessageId()) {
            fail("Right record not found, " + ourRecord.key() + " != " + record.getExternallMessageId());
        }

        if (ourRecord.key() != externalMessageId) {
            fail("Right record not found, " + ourRecord.key() + " != " + externalMessageId);
        }

        return (record);

    }

    private MessageIFace receiveJsonPowercoMessage(String topic, long externalMessageId) throws Exception {

        ConsumerRecord<Long, String> ourRecord = getNextPowercoRecord(topic);

        if (ourRecord == null) {
            fail("receiveJsonMessage == null");
        }

        String[] recordAsCSV = ourRecord.value().split(",");

        recordAsCSV[2] = new String(Base64.getDecoder().decode(recordAsCSV[2].getBytes()));

        MessageIFace record = jsonEncoder.decode(recordAsCSV[2]);

        if (ourRecord.key() != record.getExternallMessageId()) {
            fail("Right record not found, " + ourRecord.key() + " != " + record.getExternallMessageId());
        }

        if (ourRecord.key() != externalMessageId) {
            fail("Right record not found, " + ourRecord.key() + " != " + externalMessageId);
        }

        return (record);

    }

    private ConsumerRecord<Long, String> getNextDeviceRecord(String topic) {

        final ConsumerRecords<Long, String> consumerRecords = kafkaDeviceConsumer.poll(5000);

        Iterator<ConsumerRecord<Long, String>> i = consumerRecords.iterator();

        if (i.hasNext()) {
            ConsumerRecord<Long, String> aRecord = i.next();
            return aRecord;

        }

        return null;
    }

    private ConsumerRecord<Long, String> getNextPowercoRecord(String topic) {

        final ConsumerRecords<Long, String> consumerRecords = kafkaPowercoConsumer.poll(10000);

        Iterator<ConsumerRecord<Long, String>> i = consumerRecords.iterator();

        if (i.hasNext()) {
            ConsumerRecord<Long, String> aRecord = i.next();
            return aRecord;

        }

        return null;
    }

    private int drainTopic(String topic) {

        int howMany = 0;

        final ConsumerRecords<Long, String> consumerRecords = kafkaDeviceConsumer.poll(10000);

        howMany = consumerRecords.count();

        if (howMany > 0) {
            msg("Drained " + howMany + " records");
        }

        return howMany;
    }

    long testProvison(long powerCo) {

        nextDeviceId++;

        try {
            ClientResponse cr = c.callProcedure("ProvisionDevice", nextDeviceId,
                    TestSendDownstreamWithVolt.TEST_METER_NAME, TestSendDownstreamWithVolt.TEST_LOCATION, powerCo);

            checkResponseOK(cr);

        } catch (IOException | ProcCallException e) {
            fail(e.getMessage());
        }

        return nextDeviceId;
    }

    private Consumer<Long, String> connectToKafkaConsumer(String commaDelimitedHostnames, String keyDeserializer,
            String valueSerializer) throws Exception {

        String[] hostnameArray = commaDelimitedHostnames.split(",");

        StringBuffer kafkaBrokers = new StringBuffer();
        for (int i = 0; i < hostnameArray.length; i++) {
            kafkaBrokers.append(hostnameArray[i]);
            kafkaBrokers.append(":9092");

            if (i < (hostnameArray.length - 1)) {
                kafkaBrokers.append(',');
            }
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBrokers.toString());
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
//        props.put("key.deserializer", keyDeserializer);
//        props.put("value.deserializer", keyDeserializer);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // props.put("auto.offset.reset","earliest");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer" + startMs);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, VoltDBKafkaPartitioner.class.getName());

        Consumer<Long, String> newConsumer = new KafkaConsumer<>(props);

        msg("Connected to VoltDB via Kafka");

        return newConsumer;

    }

    private static Producer<Long, String> connectToKafkaProducer(String commaDelimitedHostnames, String keySerializer,
            String valueSerializer) throws Exception {

        String[] hostnameArray = commaDelimitedHostnames.split(",");

        StringBuffer kafkaBrokers = new StringBuffer();
        for (int i = 0; i < hostnameArray.length; i++) {
            kafkaBrokers.append(hostnameArray[i]);
            kafkaBrokers.append(":9092");

            if (i < (hostnameArray.length - 1)) {
                kafkaBrokers.append(',');
            }
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBrokers.toString());
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", keySerializer);
        props.put("value.serializer", valueSerializer);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, VoltDBKafkaPartitioner.class.getName());

        Producer<Long, String> newProducer = new KafkaProducer<>(props);

        msg("Connected to VoltDB via Kafka");

        return newProducer;

    }

    private static Client connectVoltDB(String commaDelimitedHostnames) throws Exception {
        Client client = null;
        ClientConfig config = null;

        try {
            msg("Logging into VoltDB");

            config = new ClientConfig(); // "admin", "idontknow");
            config.setTopologyChangeAware(true);
            config.setReconnectOnConnectionLoss(true);

            client = ClientFactory.createClient(config);

            String[] hostnameArray = commaDelimitedHostnames.split(",");

            for (String element : hostnameArray) {
                msg("Connect to " + element + "...");
                try {
                    client.createConnection(element);
                } catch (Exception e) {
                    msg(e.getMessage());
                }
            }

            msg("Connected to VoltDB");

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("VoltDB connection failed.." + e.getMessage(), e);
        }

        return client;

    }

    /**
     * Print a formatted message.
     *
     * @param message
     */
    public static void msg(String message) {

        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + message);

    }
}
