package org.voltse.test;

import static org.junit.jupiter.api.Assertions.fail;

import java.text.SimpleDateFormat;
import java.time.Duration;
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
import org.voltdb.client.topics.VoltDBKafkaPartitioner;
import org.voltse.edge.edgeencoders.JsonEncoderImpl;
import org.voltse.edge.edgeencoders.ModelEncoderIFace;
import org.voltse.edge.edgemessages.MessageIFace;

import edgeprocs.ReferenceData;

public class PowerCoEmulator {

    private static final int POLL_DELAY = 5000;
    Consumer<Long, String> kafkaPowercoConsumer;
    Producer<Long, String> kafkaProducer;

    ModelEncoderIFace jsonEncoder = new JsonEncoderImpl();

    final long powerCoEmulatorId = System.currentTimeMillis();

    public PowerCoEmulator() throws Exception {
        super();
        connectToKafkaConsumerAndProducer();
    }

    private void connectToKafkaConsumerAndProducer() {
        try {

            kafkaPowercoConsumer = connectToKafkaConsumer("localhost",
                    "org.apache.kafka.common.serialization.LongDeserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");

            kafkaPowercoConsumer.subscribe(Collections.singletonList(ReferenceData.POWERCO_1_TOPIC));

        } catch (Exception e) {
            msg(e.getMessage());
            fail(e);

        }

        try {
            kafkaProducer = connectToKafkaProducer("localhost", "org.apache.kafka.common.serialization.LongSerializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
        } catch (Exception e) {
            msg(e.getMessage());
            fail(e);

        }
    }

    public void sendMessageDownstream(String topicname, long testOwner, MessageIFace message) throws Exception {

        String encodedMessage = jsonEncoder.encode(message);

        String payload = message.getDeviceId() + "," + testOwner + ","
                + Base64.getEncoder().encodeToString(encodedMessage.getBytes());
        final ProducerRecord<Long, String> record = new ProducerRecord<>(topicname, message.getDeviceId(), payload);

        kafkaProducer.send(record).get();

    }

    public MessageIFace receiveJsonPowercoMessage(long externalMessageId) throws Exception {

        ConsumerRecord<Long, String> ourRecord = getNextPowercoRecord(externalMessageId);

        if (ourRecord == null) {
            fail("receiveJsonMessage == null");
        }

        String[] recordAsCSV = ourRecord.value().split(",");

        recordAsCSV[3] = new String(Base64.getDecoder().decode(recordAsCSV[3].getBytes()));

        MessageIFace record = jsonEncoder.decode(recordAsCSV[3]);

        if (ourRecord.key() != record.getExternallMessageId()) {
            fail("key mismatch, " + ourRecord.key() + " != " + record.getExternallMessageId());
        }

        if (ourRecord.key() != externalMessageId && externalMessageId != Long.MIN_VALUE) {
            fail("Right record not found, " + ourRecord.key() + " != " + externalMessageId);
        }

        return (record);

    }

    private ConsumerRecord<Long, String> getNextPowercoRecord(long messageId) {

        ConsumerRecords<Long, String> consumerRecords = null;

        long startMs = System.currentTimeMillis();

        long startPoll = System.currentTimeMillis();
        consumerRecords = kafkaPowercoConsumer.poll(Duration.ofMillis(POLL_DELAY));
        kafkaPowercoConsumer.commitAsync();

        if (startPoll + 30 < System.currentTimeMillis()) {
            msg("took " + (System.currentTimeMillis() - startPoll));
        }

        msg("rows=" + consumerRecords.count());

        Iterator<ConsumerRecord<Long, String>> i = consumerRecords.iterator();

        int howMany = 0;

        while (i.hasNext()) {
            howMany++;
            ConsumerRecord<Long, String> aRecord = i.next();

            if (aRecord.key() == messageId || messageId == Long.MIN_VALUE) {
                msg("OK:" + aRecord.toString());
                msg("took " + (System.currentTimeMillis() - startMs) + "ms howmany=" + howMany);
                return aRecord;
            }

        }

        fail("No ConsumerRecord found");
        return null;
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
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers.toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer" + powerCoEmulatorId);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, VoltDBKafkaPartitioner.class.getName());

        Consumer<Long, String> newConsumer = new KafkaConsumer<>(props);

        msg("Connected to VoltDB via Kafka");

        return newConsumer;

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

    public static void main(String[] args) {
        try {
            PowerCoEmulator p = new PowerCoEmulator();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void close() {

    }

    public void drain() {

        msg("PowerCoEmulator drain");
        long startPoll = System.currentTimeMillis();
        @SuppressWarnings("unused")
        ConsumerRecords<Long, String> consumerRecords = kafkaPowercoConsumer.poll(Duration.ofMillis(5000));
        int howMany = consumerRecords.count();

        while (consumerRecords != null && consumerRecords.count() > 0) {
            consumerRecords = kafkaPowercoConsumer.poll(Duration.ofMillis(1));
            howMany += consumerRecords.count();
        }

        if (startPoll + 30 < System.currentTimeMillis()) {
            msg("drain took " + (System.currentTimeMillis() - startPoll) + "ms");
        }

        if (howMany > 1) {
            msg("drained " + howMany + " records");
        }

        kafkaPowercoConsumer.commitSync();

    }

}
