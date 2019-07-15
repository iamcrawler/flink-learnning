package cn.crawler.mft_seconed.demo1;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * Created by liuliang
 * on 2019/6/19
 */
public class KafkaDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //配置正好执行一次策略
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "172.19.141.60:31090");
        prop.setProperty("group.id", "flink-group");
        FlinkKafkaConsumer011 myConsumer = new FlinkKafkaConsumer011("mysqltest", new SimpleStringSchema(), prop);
        myConsumer.setStartFromGroupOffsets();//默认消费策略
        DataStreamSource<String> source = env.addSource(myConsumer);
        source.addSink(new PrintSinkFunction());
        env.execute("StreamingFromCollection");

    }

}
