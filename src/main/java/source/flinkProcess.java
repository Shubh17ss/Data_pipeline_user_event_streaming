package source;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.*;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import source.DAO.UserActivityEvent;

public class flinkProcess {
    private static final Logger LOG = LoggerFactory.getLogger(flinkProcess.class);

    public static void main(String args[]) throws Exception{
        LOG.info("Starting Flink Job...");
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        KafkaSource<String> source=KafkaSource.<String>builder()
        .setBootstrapServers("kafka:29092")
        .setTopics("user-activity")
        .setGroupId("Flink-Group-1")
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .setStartingOffsets(OffsetsInitializer.earliest())
        .build();

        DataStream<String> rawStream=env.fromSource(source,WatermarkStrategy.noWatermarks(),"Kafka source");
        LOG.info("Source created, processing stream...");

        ObjectMapper mapper=new ObjectMapper();
        
        DataStream<UserActivityEvent> events=rawStream.map(json -> {
            LOG.info("Processing event: {}", json);
            return mapper.readValue(json, UserActivityEvent.class);
        });
        
        events.map(json -> mapper.writeValueAsString(json)).print();
        env.execute("Flink User Activity Event Processing");
    }
}
