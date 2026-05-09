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
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
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

public class flinkUserClicksFlag {
    private static final Logger LOG = LoggerFactory.getLogger(flinkUserClicksFlag.class);
    public static void main(String args[]) throws Exception{
        ObjectMapper mapper=new ObjectMapper();
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        KafkaSource<String> source=KafkaSource.<String>builder()
        .setBootstrapServers("kafka:29092")
        .setTopics("user-activity")
        .setGroupId("Flink-Group-1")
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

        DataStream<String> rawStream=env.fromSource(source, WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((event,timestamp)->{
            try{
                return mapper.readValue(event,UserActivityEvent.class).getTimestamp();
            }catch(Exception e){
                return System.currentTimeMillis();
            }
        }), "Kafka source");

        // Map JSON strings to UserActivityEvent objects
        DataStream<UserActivityEvent> timedEventStream=rawStream.map(json->mapper.readValue(json,UserActivityEvent.class));
        
        DataStream<UserActivityEvent> clickedDataStream=timedEventStream.filter(event->"click".equals(event.getEventType()));

        KeyedStream<UserActivityEvent, String> keyedClickedDataStream=clickedDataStream.keyBy(UserActivityEvent::getUserId);

        DataStream<String> alerts=keyedClickedDataStream.window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(5))) //
        .process(new ProcessWindowFunction<UserActivityEvent, String, String, TimeWindow>() {
            @Override
            public void process(String userId, ProcessWindowFunction<UserActivityEvent, String, String, TimeWindow>.Context context, Iterable<UserActivityEvent> events,  org.apache.flink.util.Collector<String> out) throws Exception {
                long clickCount=0;
                for(UserActivityEvent event:events){
                    clickCount++;
                }
                if(clickCount>10){
                    out.collect("User "+userId+" has clicked "+clickCount+" times in the last 30 seconds.");
                }
            }
        });

        alerts.map(json->{
            LOG.info("Alert: "+json);
            return json;
        });

        env.execute("Flink User Clicks Flag");
    }
}
