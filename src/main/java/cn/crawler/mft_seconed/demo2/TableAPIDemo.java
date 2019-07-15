package cn.crawler.mft_seconed.demo2;

import cn.crawler.mft_seconed.KafkaEntity;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Properties;

/**
 * Created by liuliang
 * on 2019/7/13
 */
public class TableAPIDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //配置正好执行一次策略
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "172.19.141.60:31090");
        prop.setProperty("group.id", "flink-group");
        FlinkKafkaConsumer011 myConsumer = new FlinkKafkaConsumer011("test_sql", new SimpleStringSchema(), prop);
        myConsumer.setStartFromGroupOffsets();//默认消费策略
        DataStreamSource<String> kafkaSource = env.addSource(myConsumer);

        //将kafka中取出的数据流映射为operator
        SingleOutputStreamOperator<KafkaEntity> map = kafkaSource.map(new MapFunction<String, KafkaEntity>() {
            private static final long serialVersionUID = 1471936326697828381L;
            @Override
            public KafkaEntity map(String value) {
                KafkaEntity kafkaEntity = JSON.parseObject(value, KafkaEntity.class);
                return kafkaEntity;
            }
        });

        map.print(); //打印operator

        //注册为mft_flink_kafka 表 从map里面取值，字段分别是id,message
        tableEnv.registerDataStream("mft_flink_kafka",map,"id,message");

        Table sqlQuery = tableEnv.sqlQuery("select id,message from mft_flink_kafka");

        //sink to mysql
        DataStream<Tuple2<String,String>> appendStream = tableEnv.toAppendStream(sqlQuery, Types.TUPLE(Types.STRING, Types.STRING));

        appendStream.print();
        
        appendStream.map(new MapFunction<Tuple2<String,String>, KafkaEntity>() {
            @Override
            public KafkaEntity map(Tuple2<String, String> stringStringTuple2) throws Exception {
                return new KafkaEntity(stringStringTuple2.f0,stringStringTuple2.f1);
            }
        }).addSink(new SinkKafkaEntity2Mysql());

        env.execute("kafkaEntity from Kafka");

    }
}
