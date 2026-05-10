// makes use of KeyedProcessFunction to implement session timeout
package source;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import com.fasterxml.jackson.databind.ObjectMapper;

import source.DAO.UserActivityEvent;

//imports for keyed process function and timers
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

import org.apache.flink.util.Collector;




public class flinkSessionTimeOut {
    private static final Logger LOG = LoggerFactory.getLogger(flinkSessionTimeOut.class);

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        // Set up the execution environment and Kafka source as in flinkUserClicksFlag
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // checkpoint every 5 seconds

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("user-activity")
                .setGroupId("Flink-Group-1")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawInputStream = env.fromSource(source,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> {
                            try {
                                return mapper.readValue(event, UserActivityEvent.class).getTimestamp();
                            } catch (Exception e) {
                                return System.currentTimeMillis();
                            }
                        }),
                "Kafka source");

        DataStream<UserActivityEvent> userActivityStream = rawInputStream
                .map(json -> mapper.readValue(json, UserActivityEvent.class));

    
       // Further processing to implement session timeout logic would go here, using KeyedProcessFunction to track user sessions and emit timeout events as needed.
        KeyedStream<UserActivityEvent, String> keyedStream=userActivityStream.keyBy(UserActivityEvent::getUserId);

        DataStream<String> sessionTimedOutAlert=keyedStream.process(
                new KeyedProcessFunction<String, UserActivityEvent, String>() {
                    private transient ValueState<Long> lastActivityTimestate;
                    @Override
                    public void open(Configuration parameters) throws Exception{
                        ValueStateDescriptor<Long> descriptor=new ValueStateDescriptor<>("lastActivityTime", Long.class);
                        lastActivityTimestate=getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(UserActivityEvent event, Context ctx, Collector<String> out) throws Exception{
                        Long lastActivityTimeStamp=lastActivityTimestate.value();
                        Long currentActivityTimeStamp=event.getTimestamp();

                        out.collect("Received event for user:"+ event.getUserId() + " at timestamp: "+ currentActivityTimeStamp);
                        if(lastActivityTimeStamp!=null){
                            ctx.timerService().deleteEventTimeTimer(lastActivityTimeStamp+30000); // delete previous timer if exists when new event arrives 
                        }
                        Long newTimer=currentActivityTimeStamp+30000; // set new timer for 30 seconds after current event
                        ctx.timerService().registerEventTimeTimer(newTimer);
                        lastActivityTimestate.update(currentActivityTimeStamp);

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception{
                        out.collect("Session timed out for user:"+ctx.getCurrentKey()+" at timestamp: "+timestamp);
                        lastActivityTimestate.clear(); // clear state after session timeout
                    }
                });

            // For demonstration, we can print the session timeout alerts to the console
            sessionTimedOutAlert.map(json->{
                LOG.info(json);
                return json;
            });

        env.execute("Flink Session Timeout Detection");
    }
}
