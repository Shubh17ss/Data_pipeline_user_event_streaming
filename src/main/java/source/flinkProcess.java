package source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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

    public static void main(String args[]) throws Exception {
        LOG.info("Starting Flink Job...sopln-shubh");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("user-activity")
                .setGroupId("Flink-Group-1")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStream<String> rawStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source");
        LOG.info("Source created, processing stream...");

        ObjectMapper mapper = new ObjectMapper();

        DataStream<UserActivityEvent> eventStream = rawStream.map(json -> {
            try {
                LOG.info("Filtering out null json values");
                return mapper.readValue(json, UserActivityEvent.class);
            } catch (Exception e) {
                LOG.error("Failed to parse JSON: " + json, e);
                return null; // Skip malformed events
            }
        }).filter(event -> event != null); // Filter out nulls from parsing errors

        KeyedStream<UserActivityEvent, String> resultStream = eventStream.keyBy(event -> event.getUserId());

        DataStream<String> counterStream = resultStream.map(new RichMapFunction<UserActivityEvent, String>() {
            private transient ValueState<Integer> eventCount;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                        "event-Count", // state name
                        Integer.class // type of the state
                );
                eventCount = getRuntimeContext().getState(descriptor);
            }

            @Override
            public String map(UserActivityEvent event) throws Exception {
                Integer count = eventCount.value();
                if (count == null) {
                    count = 0;
                }
                count++;
                eventCount.update(count);
                return "User: " + event.getUserId() + ", Event Count: " + count;
            }
        });

        counterStream.map(record -> {
            LOG.info("Counter Output: {}", record);
            return record;
        });

        env.execute("Flink User Activity Event Processing");
    }
}
