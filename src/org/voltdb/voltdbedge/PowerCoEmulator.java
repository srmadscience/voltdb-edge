package org.voltdb.voltdbedge;

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

public class PowerCoEmulator {
    
    private static final int ATTEMPTS = 3;
    Consumer<Long, String> kafkaPowercoConsumer;
    Producer<Long, String> kafkaProducer;


    ModelEncoderIFace jsonEncoder = new JsonEncoderImpl();
    
    final long startMs = System.currentTimeMillis();
    
    
    public PowerCoEmulator() throws Exception {   
        super();
        connectToKafkaConsumerAndProducer();
     }
    
    private void connectToKafkaConsumerAndProducer() {
        try {
 
            kafkaPowercoConsumer = connectToKafkaConsumerEarliest("localhost",
                    "org.apache.kafka.common.serialization.LongDeserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");

            kafkaPowercoConsumer.subscribe(Collections.singletonList(ReferenceData.POWERCO_1_TOPIC));

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

    
  public void sendMessageDownstream(String topicname, long testOwner, MessageIFace message) throws Exception {
        

        String encodedMessage = jsonEncoder.encode(message);

        String payload = message.getDeviceId() + "," + testOwner + ","
                + Base64.getEncoder().encodeToString(encodedMessage.getBytes());
        final ProducerRecord<Long, String> record = new ProducerRecord<>(topicname, message.getDeviceId(), payload);

        kafkaProducer.send(record).get();

    }
  

    public MessageIFace receiveJsonPowercoMessage(String topic, long externalMessageId) throws Exception {

        ConsumerRecord<Long, String> ourRecord = getNextPowercoRecord(topic, externalMessageId);

        if (ourRecord == null) {
            fail("receiveJsonMessage == null");
        }

        String[] recordAsCSV = ourRecord.value().split(",");

        recordAsCSV[3] = new String(Base64.getDecoder().decode(recordAsCSV[3].getBytes()));

        MessageIFace record = jsonEncoder.decode(recordAsCSV[3]);

        if (ourRecord.key() != record.getExternallMessageId()) {
            fail("Right record not found, " + ourRecord.key() + " != " + record.getExternallMessageId());
        }

        if (ourRecord.key() != externalMessageId) {
            fail("Right record not found, " + ourRecord.key() + " != " + externalMessageId);
        }

        return (record);

    }

    
    private ConsumerRecord<Long, String> getNextPowercoRecord(String topic, long messageId) {

        

        for (int j = 0; j < ATTEMPTS; j++) {
            
            long startMs = System.currentTimeMillis();

            long startPoll = System.currentTimeMillis();
            final ConsumerRecords<Long, String> consumerRecords = kafkaPowercoConsumer.poll(Duration.ofMillis(10000));

            if (startPoll + 30 < System.currentTimeMillis()) {
                msg(j+ ": took " + (System.currentTimeMillis() - startPoll));
            }

            Iterator<ConsumerRecord<Long, String>> i = consumerRecords.iterator();

            while (i.hasNext()) {
                ConsumerRecord<Long, String> aRecord = i.next();

                if (aRecord.key() == messageId || messageId == Long.MIN_VALUE) {
                    msg("OK:" + aRecord.toString());
                    msg("pass=  "+j+ " took " + (System.currentTimeMillis() - startMs) + " ms");
                    return aRecord;
                } else {
                    msg(aRecord.toString());
                }

            }

        }
        msg("FAILED and took " + (System.currentTimeMillis() - startMs) + " ms");
        return null;
    }
    
    private Consumer<Long, String> connectToKafkaConsumerEarliest(String commaDelimitedHostnames,
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
        props.put("bootstrap.servers", kafkaBrokers.toString());
        props.put("auto.commit", true);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put("auto.offset.reset", "earliest");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer" + startMs);
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
            PowerCoEmulator p  = new PowerCoEmulator();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void close() {
      

        
       
        
    }


}
