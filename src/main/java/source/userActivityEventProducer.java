package source;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import source.DAO.UserActivityEvent;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class userActivityEventProducer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "user-activity";
    private static final ObjectMapper objectMapper = new ObjectMapper();


    public static void main(String[] args) throws Exception {
        // Configure the Kafka producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

    
        int numberOfThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

        try {
            // Submit multiple tasks to the thread pool
            for (int i = 0; i < numberOfThreads; i++) {
                executor.execute(() -> {
                    try {
                        sendUserActivityEvents(props);
                    } catch (Exception e) {
                        Thread.currentThread().interrupt();
                        e.printStackTrace();
                    }
                });
            }
            // Generate and send user activity events
        } catch (Exception e) {
            System.err.println("Error sending events: " + e.getMessage());
            e.printStackTrace();
        } finally {
            executor.awaitTermination(Long.MAX_VALUE, java.util.concurrent.TimeUnit.NANOSECONDS);
        }
    }

    private static void sendUserActivityEvents(Properties props)
            throws ExecutionException, InterruptedException {
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
            
        String[] eventTypes = {"page_view", "click", "scroll", "form_submit", "logout", "login"};
        String[] pageIds = {"home", "product", "checkout", "cart", "profile", "settings", "dashboard"};
        int[] userIds = {101, 102, 103, 104, 105, 106, 107, 108, 109, 110};

        Random random = new Random();

        System.out.println("Starting to send user activity events to Kafka...");

        while (true) {
            // Generate random event data
            String eventId=String.valueOf(random.nextInt(0,Integer.MAX_VALUE));
            String userId = String.valueOf(userIds[random.nextInt(userIds.length)]);
            String eventType = eventTypes[random.nextInt(eventTypes.length)];
            String pageId = pageIds[random.nextInt(pageIds.length)];
            long timestamp = System.currentTimeMillis();

            // Create the event object
            UserActivityEvent event = new UserActivityEvent(userId,eventId,eventType, pageId, timestamp);

            try {
                // Convert event to JSON string
                String eventJson = objectMapper.writeValueAsString(event);

                // Create producer record with userId as key
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, userId, eventJson);

                // Send the record and get metadata
                RecordMetadata metadata = producer.send(record).get();

                System.out.println("Thread "+Thread.currentThread().getName()+" EventId " + eventId + " sent successfully - " +
                        "Topic: " + metadata.topic() +
                        ", Partition: " + metadata.partition() +
                        ", Offset: " + metadata.offset() +
                        ", UserId: " + userId +
                        ", EventType: " + eventType);

            } catch (Exception e) {
                System.err.println("Error sending event " + eventId + ": " + e.getMessage());
                e.printStackTrace();
            }

            // Small delay between events (optional)
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Flush any remaining events
        producer.flush();
        System.out.println("All events sent and flushed successfully!");
    }
}
