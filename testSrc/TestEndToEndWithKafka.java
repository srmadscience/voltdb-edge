import static org.junit.jupiter.api.Assertions.fail;

import java.text.SimpleDateFormat;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.voltdb.client.topics.VoltDBKafkaPartitioner;

class TestEndToEndWithKafka {
    
    final long startMs = System.currentTimeMillis();
    
    Consumer kafkaConsumer ;
    Producer kafkaProducer ;

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
    }

    @BeforeEach
    void setUp() throws Exception {
        
        try {
            kafkaConsumer = connectToKafkaConsumer("localhost","org.apache.kafka.common.serialization.StringDeserializer",
                    "TestDeserializer");
        } catch (Exception e) {
            msg(e.getMessage());
            throw(e);
        }

        try {
            kafkaProducer = connectToKafkaProducer("localhost","org.apache.kafka.common.serialization.StringSerializer",
                    "TestSerializer");
        } catch (Exception e) {
            msg(e.getMessage());
            throw(e);
        }

    }

    @AfterEach
    void tearDown() throws Exception {
        
        kafkaConsumer.close();
        kafkaConsumer = null;
        kafkaProducer.close();
        kafkaProducer = null;
    }

    @Test
    void test() {
        
        try {
            
            ConsumerRecord ourRecord = getNextRecord("segment_1_topic");
            
            
            
        } catch (Exception e) {
            fail(e);
        }
        
        
        fail("Not yet implemented");
    }

    private ConsumerRecord getNextRecord(String topic) {
        
        kafkaConsumer.subscribe(Collections.singletonList(topic));
         
        final ConsumerRecords<String, String> consumerRecords =
                kafkaConsumer.poll(10000);
        
        Iterator<ConsumerRecord<String,String>> i = consumerRecords.iterator();
        
        
        if (i.hasNext()) {
            ConsumerRecord<String,String> aRecord = i.next();
            return aRecord;
            
        }
        
        return null;
    }

   private int drainTopic(String topic) {
       
        int howMany = 0;
        
        kafkaConsumer.subscribe(Collections.singletonList(topic));
         
        final ConsumerRecords<String, String> consumerRecords =
                kafkaConsumer.poll(10000);
        
       howMany = consumerRecords.count();
       
       if (howMany > 0) {
           msg("Drained " + howMany + " records");
       }
        
        
        return howMany;
    }


   
    private  Consumer<Long, String> connectToKafkaConsumer(String commaDelimitedHostnames, String keyDeserializer,
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
        props.put("key.deserializer", keyDeserializer);
        props.put("value.deserializer", keyDeserializer);
        props.put("auto.offset.reset","earliest");
        
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaExampleConsumer"+startMs);
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
}
