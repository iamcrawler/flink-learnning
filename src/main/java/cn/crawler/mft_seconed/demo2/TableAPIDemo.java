package cn.crawler.mft_seconed.demo2;

import cn.crawler.mft_seconed.KafkaEntity;
import cn.crawler.mft_seconed.KafkaEntityTableSource;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.WindowGroupedTable;
import org.apache.flink.table.api.java.StreamTableEnvironment;


import java.lang.reflect.Type;
import java.util.Properties;

/**
 * Created by liuliang
 * on 2019/7/13
 */
public class TableAPIDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime); // default
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.简单的kafka->flink sql->mysql
        //配置正好执行一次策略
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //1S执行一次checkpoint
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "172.19.141.60:31090");
        prop.setProperty("group.id", "flink-group");
        FlinkKafkaConsumer011 myConsumer = new FlinkKafkaConsumer011("fan_or_jia", new SimpleStringSchema(), prop);
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
        //注册为mft_flink_kafka 表 从map里面取值，字段分别是id,message,name,time
        //数据流无缝转换为table
        tableEnv.registerDataStream("mft_flink_kafka",map,"id,message,name,create_time");


        Table sqlQuery = tableEnv.sqlQuery("select id,message,name,create_time from mft_flink_kafka");


//        Table table = tableEnv.scan("mft_flink_kafka");
//        Table table1 = table.filter("id%2!=0")
////                .join()
////                .leftOuterJoin()
////                .where("id === 10")
////                .window(Tumble.over("1000.millis").on("rowtime").as("total"))
//                .groupBy("id")
//                .select("id.sum as sumId,name,message,create_time");


        //sink to mysql
        DataStream<Tuple4<Integer,String,String,Long>> appendStream = tableEnv.toAppendStream(sqlQuery, Types.TUPLE(Types.INT, Types.STRING,Types.STRING, Types.LONG));

        appendStream.print();

        appendStream.map(new MapFunction<Tuple4<Integer,String,String,Long>, KafkaEntity>() {
            @Override
            public KafkaEntity map(Tuple4<Integer, String,String,Long> stringStringTuple4) throws Exception {
                return new KafkaEntity(stringStringTuple4.f0,stringStringTuple4.f1,stringStringTuple4.f2,stringStringTuple4.f3);
            }
        }).addSink(new SinkKafkaEntity2Mysql());

//        2.table api 带窗口函数的，处理时间属性由KafkaEntityTableSource实现DefinedProctimeAttribute接口的定义。逻辑时间属性附加到由返回类型定义的物理模式TableSource。
//        tableEnv.registerDataStream("kafka_demo",new KafkaEntityTableSource());


        env.execute("kafkaEntity from Kafka");

    }
}
