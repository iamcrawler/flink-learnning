package cn.crawler.mft_seconed.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.runtime.operators.TimestampsAndPeriodicWatermarksOperator;

import java.util.Properties;

public class KafkaSource {

    private static final String kafkaHost = "172.19.141.60:31090";

    public static FlinkKafkaConsumer011 getKafkaSource(String topic){

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", kafkaHost);
        prop.setProperty("group.id", "flink-group");
        FlinkKafkaConsumer011 myConsumer = new FlinkKafkaConsumer011(topic, new SimpleStringSchema(), prop);
        myConsumer.setStartFromGroupOffsets();//默认消费策略
        return myConsumer;

    }

}
