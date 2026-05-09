package source;

import org.apache.flink.configuration.Configuration;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.connector.kafka.source.*;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.datastream.DataStream.Collector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import com.fasterxml.jackson.databind.ObjectMapper;

import source.DAO.UserActivityEvent;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class flinkEventTimeProcess {
    private static final Logger LOG = LoggerFactory.getLogger(flinkEventTimeProcess.class);

    public static void main(String args[]) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("user-activity")
                .setGroupId("Flink-Group-1")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        // Assign timestamps and watermarks based on event time with a 5-second
        // out-of-orderness bound
        DataStream<String> rawStream = env.fromSource(source,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> {
                            try {
                                return mapper.readValue(event, UserActivityEvent.class).getTimestamp();
                            } catch (Exception e) {
                                return System.currentTimeMillis();
                            }
                        }),
                "Kafka source");

        DataStream<UserActivityEvent> timedEventStream = rawStream.map(json -> {
            try {
                LOG.info("Filtering out null json values");
                return mapper.readValue(json, UserActivityEvent.class);
            } catch (Exception e) {
                LOG.error("Failed to parse JSON: " + json, e);
                return null; // Skip malformed events
            }
        });

        // Count events per user in 1-minute tumbling windows
        KeyedStream<UserActivityEvent, String> keyedStream = timedEventStream
                .filter(event -> event != null)
                .keyBy(UserActivityEvent::getUserId);
        DataStream<String> counterStream = keyedStream.window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new ProcessWindowFunction<UserActivityEvent, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                            ProcessWindowFunction<UserActivityEvent, String, String, TimeWindow>.Context context,
                            Iterable<UserActivityEvent> elements,
                            org.apache.flink.util.Collector<String> out) throws Exception {
                        long count = 0;
                        for (UserActivityEvent event : elements) {
                            count++;
                        }
                        out.collect("User: " + key + ", Count: " + count + ", Window: " + context.window().getStart()
                                + " - " + context.window().getEnd());
                    }
                });

        counterStream.map(record -> {
            LOG.info("Window Result: {}", record);
            return record;
        });

        env.execute("Flink User Activity Event Processing");
    }
}
