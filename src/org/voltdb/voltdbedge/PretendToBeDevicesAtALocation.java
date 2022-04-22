package org.voltdb.voltdbedge;

import static org.junit.jupiter.api.Assertions.fail;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;

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
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.topics.VoltDBKafkaPartitioner;
import org.voltse.edge.edgeencoders.JsonEncoderImpl;
import org.voltse.edge.edgeencoders.ModelEncoderIFace;
import org.voltse.edge.edgeencoders.TabEncoderImpl;
import org.voltse.edge.edgemessages.DisableFeatureMessage;
import org.voltse.edge.edgemessages.EnableFeatureMessage;
import org.voltse.edge.edgemessages.GetStatusMessage;
import org.voltse.edge.edgemessages.MessageIFace;
import org.voltse.edge.edgemessages.StartMessage;
import org.voltse.edge.edgemessages.StopMessage;
import org.voltse.edge.edgemessages.UpgradeFirmwareMessage;

import com.google.gson.Gson;

import edgeprocs.ReferenceData;

public class PretendToBeDevicesAtALocation implements Runnable {

    private static final long POLL_DELAY = 100;
    private static final long ONE_MINUTE_MS = 60000;

    Client mainClient;
    String hostnames;
    int location;
    int duration;
    long startMs = System.currentTimeMillis();
    HashMap<Long, Device> deviceMap = new HashMap<Long, Device>();
    long[] deviceIds = null;
    HashMap<String, ModelEncoderIFace> encoders = new HashMap<String, ModelEncoderIFace>();
    Consumer<Long, String> kafkaDeviceConsumer;
    Producer<Long, String> kafkaProducer;
    final long powerCoEmulatorId = System.currentTimeMillis();
    JsonEncoderImpl jsonenc = new JsonEncoderImpl();
    TabEncoderImpl tabenc = new TabEncoderImpl();
    Random r = new Random();
    Gson g = new Gson();

    public PretendToBeDevicesAtALocation(Client mainClient, String hostnames, int duration, int location) {
        super();
        encoders.put(jsonenc.getName(), jsonenc);
        encoders.put(tabenc.getName(), tabenc);
        encoders.put("org.voltse.edge.edgeencoders.JsonEncoderImpl", jsonenc);
        encoders.put("org.voltse.edge.edgeencoders.TabEncoderImpl", tabenc);

        this.hostnames = hostnames;
        this.mainClient = mainClient;
        this.duration = duration;
        this.location = location;

        connectToKafkaConsumerAndProducer();

        try {
            getDevices(mainClient, location);
        } catch (Exception e) {
            msg(e.getMessage());
        }
    }

    @Override
    public void run() {

        long lastStatsTime = System.currentTimeMillis();
        int downstreamRecd = 0;
        int upstreamSent = 0;
        long lagMs = 0;

        while (System.currentTimeMillis() < (duration * 1000) + startMs) {
            try {

                // See if anyone has contacted us
                ConsumerRecords<Long, String> consumerRecords = kafkaDeviceConsumer.poll(Duration.ofMillis(POLL_DELAY));
                kafkaDeviceConsumer.commitAsync();

                if (consumerRecords.count() > 0) {

                    Iterator<ConsumerRecord<Long, String>> i = consumerRecords.iterator();

                    while (i.hasNext()) {
                        ConsumerRecord<Long, String> aRecord = i.next();

                        String[] recordAsCSV = aRecord.value().split(",");

                        long deviceId = Integer.parseInt(recordAsCSV[1]);
                        Device ourDevice = deviceMap.get(deviceId);

                        // msg("Device=" + aRecord.value());

                        if (ourDevice != null) {

                            downstreamRecd++;

                            ourDevice.setMeterReading(ourDevice.getMeterReading() + r.nextInt(100));

                            recordAsCSV[2] = new String(Base64.getDecoder().decode(recordAsCSV[2].getBytes()));

                            MessageIFace downstreamRecord = ourDevice.getEncoder().decode(recordAsCSV[2]);

                            msg("Got incoming message " + downstreamRecord.toString());

                            lagMs = System.currentTimeMillis() - downstreamRecord.getCreateDate().getTime();

                            msg(downstreamRecord.getMessageType());

                            if (downstreamRecord instanceof GetStatusMessage) {
                                GetStatusMessage ourMessage = (GetStatusMessage) downstreamRecord;
                                MeterReading mr = new MeterReading(ourMessage.getDeviceId(),
                                        ourDevice.getMeterReading());
                                ourMessage.setJsonPayload(g.toJson(mr));
                                ourMessage.setErrorMessage("OK");
                                msg(ourMessage.toString());
                                sendMessageUpstream("upstream_1_topic", ourMessage);
                                upstreamSent++;
                            } else if (downstreamRecord instanceof DisableFeatureMessage) {
                                DisableFeatureMessage ourMessage = (DisableFeatureMessage) downstreamRecord;

                                if (ourMessage.isEnabled() & (!ourDevice.getFeature(ourMessage.getFeatureName()))) {
                                    ourMessage.setErrorMessage("Already Disabled");
                                } else {
                                    ourMessage.setErrorMessage("Enabled");
                                }

                                ourDevice.setFeature(ourMessage.getFeatureName(), ourMessage.isEnabled());

                                msg(ourMessage.toString());
                                sendMessageUpstream("upstream_1_topic", ourMessage);
                                upstreamSent++;
                            } else if (downstreamRecord instanceof EnableFeatureMessage) {
                                EnableFeatureMessage ourMessage = (EnableFeatureMessage) downstreamRecord;

                                if (ourMessage.isEnabled() & ourDevice.getFeature(ourMessage.getFeatureName())) {
                                    ourMessage.setErrorMessage("Already Enabled");
                                } else {
                                    ourMessage.setErrorMessage("Enabled");
                                }

                                ourDevice.setFeature(ourMessage.getFeatureName(), ourMessage.isEnabled());
                                ourMessage.setErrorMessage("OK");
                                msg(ourMessage.toString());
                                sendMessageUpstream("upstream_1_topic", ourMessage);
                                upstreamSent++;

                            } else if (downstreamRecord instanceof StartMessage) {

                                StartMessage ourMessage = (StartMessage) downstreamRecord;
                                ourMessage.setErrorMessage("STARTED");
                                msg(ourMessage.toString());
                                sendMessageUpstream("upstream_1_topic", ourMessage);

                            } else if (downstreamRecord instanceof StopMessage) {

                                StopMessage ourMessage = (StopMessage) downstreamRecord;
                                ourMessage.setErrorMessage("STOPPED");
                                msg(ourMessage.toString());
                                sendMessageUpstream("upstream_1_topic", ourMessage);
                                upstreamSent++;

                            } else if (downstreamRecord instanceof UpgradeFirmwareMessage) {

                                UpgradeFirmwareMessage ourMessage = (UpgradeFirmwareMessage) downstreamRecord;
                                ourMessage.setErrorMessage("Upgraded " + ourMessage.getPayload().length + " bytes");
                                msg(ourMessage.toString());
                                sendMessageUpstream("upstream_1_topic", ourMessage);
                                upstreamSent++;

                            }

                        }
                    }

                }

            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            if (lastStatsTime + ONE_MINUTE_MS < System.currentTimeMillis()) {
                try {
                    reportStats(mainClient, "edge_bl_stats", "edge_bl_stats", "devicestats", "upstreamSent" + location,
                            upstreamSent);

                    reportStats(mainClient, "edge_bl_stats", "edge_bl_stats", "devicestats",
                            "downstreamRecd" + location, downstreamRecd);

                    reportStats(mainClient, "edge_bl_stats", "edge_bl_stats", "devicestats", "lagms" + location, lagMs);

                    downstreamRecd = 0;
                    upstreamSent = 0;

                    lagMs = 0;
                    lastStatsTime = System.currentTimeMillis();

                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

    }

    protected void getDevices(Client mainClient, int location)
            throws InterruptedException, IOException, NoConnectionsException {

        int deviceIdEntry = 0;

        try {
            ClientResponse cr = mainClient.callProcedure("GetDevicesForLocation", location);
            deviceIds = new long[cr.getResults()[0].getRowCount()];

            while (cr.getResults()[0].advanceRow()) {

                Device newDevice = new Device(cr.getResults()[0].getLong("DEVICE_ID"),
                        encoders.get(cr.getResults()[0].getString("encoder_class_name")),
                        cr.getResults()[0].getString("MODEL_NUMBER"));

                newDevice.setMeterReading(r.nextInt(1000000));
                deviceMap.put(newDevice.getDeviceId(), newDevice);
                deviceIds[deviceIdEntry++] = newDevice.getDeviceId();

            }

        } catch (IOException | ProcCallException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        msg("Got " + deviceIds.length + " devices");

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

    private void connectToKafkaConsumerAndProducer() {
        try {

            kafkaDeviceConsumer = connectToKafkaConsumer(hostnames,
                    "org.apache.kafka.common.serialization.LongDeserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");

            kafkaDeviceConsumer.subscribe(Collections.singletonList(ReferenceData.SEGMENT_1_TOPIC));

        } catch (Exception e) {
            msg(e.getMessage());
            fail(e);

        }

        try {
            kafkaProducer = connectToKafkaProducer(hostnames, "org.apache.kafka.common.serialization.LongSerializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
        } catch (Exception e) {
            msg(e.getMessage());
            fail(e);

        }
    }

    public void sendMessageUpstream(String topicname, MessageIFace message) {

        try {

            ModelEncoderIFace ourEncoder = jsonenc;

            if (ReferenceData.getdeviceEncoderClassName(message)
                    .equals("org.voltse.edge.edgeencoders.TabEncoderImpl")) {
                ourEncoder = tabenc;
            }

            String encodedMessage = ourEncoder.encode(message);

            String payload = message.getDeviceId() + ","
                    + Base64.getEncoder().encodeToString(encodedMessage.getBytes());
            final ProducerRecord<Long, String> record = new ProducerRecord<>(topicname, message.getDeviceId(), payload);

            kafkaProducer.send(record).get();

        } catch (Exception e) {
            fail("sendMessageUpstream:" + e.getMessage());
        }

    }

    private static void reportStats(Client c, String statname, String stathelp, String eventType, String eventName,
            long statvalue) throws IOException, NoConnectionsException, ProcCallException {
        NullCallback coec = new NullCallback();

        c.callProcedure(coec, "promBL_latency_stats.UPSERT", statname, stathelp, eventType, eventName, statvalue,
                new Date());

    }

    /**
     * Connect to VoltDB using a comma delimited hostname list.
     *
     * @param commaDelimitedHostnames
     * @return
     * @throws Exception
     */
    protected static Client connectVoltDB(String commaDelimitedHostnames) throws Exception {
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

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("VoltDB connection failed.." + e.getMessage(), e);
        }

        return client;

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

        msg("Parameters:" + Arrays.toString(args));

        if (args.length != 3) {
            msg("Usage: hostnames durationseconds location");
            System.exit(1);
        }

        String hostnames = args[0];
        int duration = Integer.parseInt(args[1]);
        int location = Integer.parseInt(args[2]);

        msg("durationSeconds=" + duration + " location=" + location);

        try {
            Client c = connectVoltDB(hostnames);

            Thread thread = new Thread(new PretendToBeDevicesAtALocation(c, hostnames, duration, location));
            thread.start();
            thread.join();

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

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

    class MeterReading {

        long deviceId;
        long readingKwh;
        Date readingTime;

        public MeterReading(long deviceId, long readingKwh) {
            super();
            this.deviceId = deviceId;
            this.readingKwh = readingKwh;
            this.readingTime = new Date();
        }
    }

}
