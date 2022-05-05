/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltse.test;

import static org.junit.jupiter.api.Assertions.fail;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
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
import org.voltse.edge.edgeencoders.TabEncoderImpl;
import org.voltse.edge.edgemessages.MessageIFace;

import edgeprocs.ReferenceData;

public class DeviceEmulator {

    private static final int POLL_DELAY = 5000;

    Consumer<Long, String> kafkaDeviceConsumer;
    Producer<Long, String> kafkaProducer;

    ModelEncoderIFace jsonEncoder = new JsonEncoderImpl();
    ModelEncoderIFace tabEncoder = new TabEncoderImpl();

    HashMap<String, ModelEncoderIFace> encoders = new HashMap<>();

    final long deviceEmulatorId = System.currentTimeMillis();

    public DeviceEmulator() throws Exception {
        super();

        encoders.put(jsonEncoder.getName(), jsonEncoder);
        encoders.put(tabEncoder.getName(), tabEncoder);

        connectToKafkaConsumerAndProducer();

    }

    private void connectToKafkaConsumerAndProducer() {
        try {
            kafkaDeviceConsumer = connectToKafkaConsumer("localhost",
                    "org.apache.kafka.common.serialization.LongDeserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");

            kafkaDeviceConsumer.subscribe(Collections.singletonList(ReferenceData.SEGMENT_1_TOPIC));

        } catch (Exception e) {
            msg(e.getMessage());

        }

        try {
            kafkaProducer = connectToKafkaProducer("localhost", "org.apache.kafka.common.serialization.LongSerializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
        } catch (Exception e) {
            msg(e.getMessage());

        }
    }

    public MessageIFace receiveDeviceMessage(long externalMessageId, String encoderName) throws Exception {

        ModelEncoderIFace ourEncoder = encoders.get(encoderName);
        ConsumerRecord<Long, String> ourRecord = getNextDeviceRecord(externalMessageId);

        if (ourRecord == null) {
            fail("receiveJsonMessage == null");
        }

        String[] recordAsCSV = ourRecord.value().split(",");

        recordAsCSV[2] = new String(Base64.getDecoder().decode(recordAsCSV[2].getBytes()));

        MessageIFace record = ourEncoder.decode(recordAsCSV[2]);

        if (ourRecord.key() != record.getExternallMessageId()) {
            fail("Right record not found, " + ourRecord.key() + " != " + record.getExternallMessageId());
        }

        if (ourRecord.key() != externalMessageId) {
            fail("Right record not found, " + ourRecord.key() + " != " + externalMessageId);
        }

        return (record);
    }

    private ConsumerRecord<Long, String> getNextDeviceRecord(long messageId) {

        long startMs = System.currentTimeMillis();

        ConsumerRecords<Long, String> consumerRecords = kafkaDeviceConsumer.poll(Duration.ofMillis(POLL_DELAY));
        int howMany = 0;

        while (consumerRecords.count() > 0) {

            Iterator<ConsumerRecord<Long, String>> i = consumerRecords.iterator();

            while (i.hasNext()) {
                howMany++;
                ConsumerRecord<Long, String> aRecord = i.next();

                if (aRecord.key() == messageId) {
                    msg("OK:" + aRecord.toString());
                    msg("took " + (System.currentTimeMillis() - startMs) + " ms howmany=" + howMany);
                    return aRecord;
                }

            }

            consumerRecords = kafkaDeviceConsumer.poll(Duration.ofMillis(POLL_DELAY));

        }
        return null;
    }

    public void sendMessageUpstream(String topicname, MessageIFace message) {

        try {
            ModelEncoderIFace ourEncoder = encoders.get(ReferenceData.getdeviceEncoding(message));

            String encodedMessage = ourEncoder.encode(message);

            String payload = message.getDeviceId() + ","
                    + Base64.getEncoder().encodeToString(encodedMessage.getBytes());
            final ProducerRecord<Long, String> record = new ProducerRecord<>(topicname, message.getDeviceId(), payload);

            kafkaProducer.send(record).get();

        } catch (Exception e) {
            fail("sendMessageUpstream:" + e.getMessage());
        }

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

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer" + deviceEmulatorId);
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

    public void drain() {

        msg("DeviceEmulator drain");
        long startPoll = System.currentTimeMillis();
        @SuppressWarnings("unused")
        ConsumerRecords<Long, String> consumerRecords = kafkaDeviceConsumer.poll(Duration.ofMillis(5000));
        int howMany = consumerRecords.count();

        while (consumerRecords != null && consumerRecords.count() > 0) {
            consumerRecords = kafkaDeviceConsumer.poll(Duration.ofMillis(1));
            howMany += consumerRecords.count();
        }

        if (startPoll + 30 < System.currentTimeMillis()) {
            msg("drain took " + (System.currentTimeMillis() - startPoll) + "ms");
        }

        if (howMany > 1) {
            msg("drained " + howMany + " records");
        }

        kafkaDeviceConsumer.commitSync();

    }

}
