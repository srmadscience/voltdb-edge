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
import org.voltse.edge.edgemessages.GetStatusMessage;
import org.voltse.edge.edgemessages.MessageIFace;
import org.voltse.edge.edgemessages.UpgradeFirmwareMessage;

import edgeprocs.ReferenceData;

public class PretendToBeAPowerCo implements Runnable {


    private static final int LOCATION_COUNT = 16;

    private static final long POLL_DELAY = 100;
    
    Client mainClient;
    String hostnames;
    int tps;
    int duration;
    int howmany;
    int queryseconds;
    int powerco;   
    long startMs = System.currentTimeMillis();
    HashMap<Long,Device> deviceMap = new HashMap<Long,Device>();
    long[] deviceIds = null;
    HashMap<String, ModelEncoderIFace> encoders = new HashMap<String, ModelEncoderIFace>();
    Consumer<Long, String> kafkaPowercoConsumer;
    Producer<Long, String> kafkaProducer;
    final long powerCoEmulatorId = System.currentTimeMillis();
    JsonEncoderImpl jsonenc = new JsonEncoderImpl();
    TabEncoderImpl tabenc = new TabEncoderImpl();
    Random r = new Random();

    public PretendToBeAPowerCo(Client mainClient, String hostnames, int tps, int duration, int howmany, int queryseconds, int powerrco) {
        super();
        encoders.put("org.voltse.edge.edgeencoders.JsonEncoderImpl", jsonenc);
        encoders.put("org.voltse.edge.edgeencoders.TabEncoderImpl", tabenc);
        
        this.hostnames = hostnames;
        this.mainClient = mainClient;
        this.tps = tps;
        this.duration = duration;
        this.howmany = howmany;
        this.queryseconds = queryseconds;
        this.powerco = powerrco;
        
        connectToKafkaConsumerAndProducer();
        
        try {
            createDevices(mainClient, howmany, tps, powerrco);
        } catch (Exception e) {
          msg(e.getMessage());
        }
    }

    
 
    @Override
    public void run() {
        
        while (System.currentTimeMillis() < (duration * 1000) + startMs) {
            try {
                
                
                // See if anyone has contacted us
                ConsumerRecords<Long, String> consumerRecords = kafkaPowercoConsumer.poll(Duration.ofMillis(POLL_DELAY));
                kafkaPowercoConsumer.commitAsync();
                
                if (consumerRecords.count() > 0) {
                    
                    Iterator<ConsumerRecord<Long, String>> i = consumerRecords.iterator();

               
                    while (i.hasNext()) {
                       
                        ConsumerRecord<Long, String> aRecord = i.next();                                         
                        String[] recordAsCSV = aRecord.value().split(",");
                        recordAsCSV[3] = new String(Base64.getDecoder().decode(recordAsCSV[3].getBytes()));
                        MessageIFace record = jsonenc.decode(recordAsCSV[3]);
                        msg("Got incoming message " + record.toString());
                        
                  }
                    
                }
                
                for (int i=0; i < tps; i++) {
                
                // find a device to talk to
                Device testDevice = deviceMap.get(deviceIds[r.nextInt(deviceIds.length)]);
              
                MessageIFace message = null;
                
                long deviceId = testDevice.getDeviceId();
                long externallMessageId = System.currentTimeMillis();
                long latencyMs = -1;
                String errorMessage = null;
                Date createDate = new Date();
                int destinationSegmentId = -1;
                long callingOwner = powerco;

                if (r.nextInt(2) == 0) {
                    
                    
                     message = new GetStatusMessage( deviceId,  externallMessageId,  latencyMs,  errorMessage,
                             createDate,  destinationSegmentId,   callingOwner, null);
               
                } else {
                    
                    byte[] payload = "Hello World".getBytes();
                    
                    message = new UpgradeFirmwareMessage( deviceId,  externallMessageId,  latencyMs,  errorMessage,
                            createDate,  destinationSegmentId,  payload,  callingOwner);
                }
                
                 
                
                testDevice.addMessage(message);
                sendMessageDownstream(ReferenceData.DOWNSTREAM_TOPIC, powerco, message) ;
                
                }
                
                
                
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
    }
 
     
   

    protected  void createDevices(Client mainClient, int howMany, int tpMs, int powerco)
            throws InterruptedException, IOException, NoConnectionsException {

        Random r = new Random();

        TransactionSpeedRegulator tsm = new TransactionSpeedRegulator(tpMs, TransactionSpeedRegulator.NO_END_DATE);

        msg("Creating " + howMany + " devices");

        for (int i = 0; i < howMany; i++) {

            try {

                int nextDeviceId = i + (10000000 * powerco);

                tsm.waitIfNeeded();

                NullCallback ncb = new NullCallback();
                
              

                ClientResponse cr = mainClient.callProcedure("ProvisionDevice", nextDeviceId,
                        ReferenceData.METER_TYPES[(nextDeviceId + 1) % 2], 0, /*r.nextInt(LOCATION_COUNT)*/ powerco);
                
                if (cr.getStatus() != ClientResponse.SUCCESS) {
                    msg(cr.getAppStatusString());
                }
                
              

            } catch (Exception e) {
                fail(e.getMessage());
            }

        }

        mainClient.drain();
        msg("Creating " + howMany + " devices ... done");
        
        deviceIds = new long[howMany];
        int deviceIdEntry = 0;
        
        try {
            ClientResponse cr = mainClient.callProcedure("GetDevicesForPowerco", powerco);
            
            while (cr.getResults()[0].advanceRow()) {
               
                Device newDevice = new Device (cr.getResults()[0].getLong("DEVICE_ID"), encoders.get(cr.getResults()[0].getString("encoder_class_name")) , cr.getResults()[0].getString("MODEL_NUMBER"));
               
                deviceMap.put(newDevice.getDeviceId(), newDevice);
                deviceIds[deviceIdEntry++] = newDevice.getDeviceId();
                
            }
            
            
        } catch (IOException | ProcCallException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        
        

    }
    
    
   
    
    private Consumer<Long, String> connectToKafkaConsumer(String commaDelimitedHostnames,
            String keyDeserializer, String valueSerializer) throws Exception {

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

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer" + powerCoEmulatorId );
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, VoltDBKafkaPartitioner.class.getName());

        Consumer<Long, String> newConsumer = new KafkaConsumer<>(props);

        msg("Connected to VoltDB via Kafka");

        return newConsumer;

    }
    
    private void connectToKafkaConsumerAndProducer() {
        try {

            kafkaPowercoConsumer = connectToKafkaConsumer("localhost",
                    "org.apache.kafka.common.serialization.LongDeserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");

            kafkaPowercoConsumer.subscribe(Collections.singletonList("powerco_" + powerco + "_topic"));

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

        String encodedMessage = jsonenc.encode(message);

        String payload = message.getDeviceId() + "," + testOwner + ","
                + Base64.getEncoder().encodeToString(encodedMessage.getBytes());
        final ProducerRecord<Long, String> record = new ProducerRecord<>(topicname, message.getDeviceId(), payload);

        kafkaProducer.send(record).get();


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

    protected static void deleteOldData(Client mainClient, int id)
            throws InterruptedException, IOException, NoConnectionsException {
        try {
            msg("DELETE FROM devices WHERE current_owner_id = " + id + ";");
            mainClient.callProcedure("@AdHoc", "DELETE FROM devices WHERE current_owner_id = " + id + ";");

        } catch (IOException | ProcCallException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {

        msg("Parameters:" + Arrays.toString(args));

        if (args.length != 6) {
            msg("Usage: hostnames  tpms durationseconds devicecount  queryseconds powerco");
            System.exit(1);
        }

        String hostnames = args[0];
        int tps = Integer.parseInt(args[1]);
        int duration = Integer.parseInt(args[2]);
        int howmany = Integer.parseInt(args[3]);
        int queryseconds = Integer.parseInt(args[4]);
        int powerrco = Integer.parseInt(args[5]);

        msg("TPS=" + tps + ", durationSeconds=" + duration + ", query interval seconds = " + queryseconds + ", howmany="
                + howmany + " powerco=" + powerrco);

        try {
            Client c = connectVoltDB(hostnames);

            deleteOldData(c, powerrco);
                     
            Thread thread = new Thread(new PretendToBeAPowerCo( c,  hostnames,  tps,  duration,  howmany,  queryseconds,  powerrco));
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



}