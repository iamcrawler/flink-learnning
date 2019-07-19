package cn.crawler.mft_seconed;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.util.Properties;

public class KafkaEntityTableSource  implements StreamTableSource<Row>, DefinedProctimeAttribute {
    @Nullable
    @Override
    public String getProctimeAttribute() {
        return "actionTime";
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {

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
        SingleOutputStreamOperator<Row> row = kafkaSource.map(new MapFunction<String, Row>() {
            private static final long serialVersionUID = 1471936326697828381L;
            @Override
            public Row map(String value) {
                KafkaEntity kafkaEntity = JSON.parseObject(value, KafkaEntity.class);
                return Row.of(kafkaEntity.getId(),kafkaEntity.getMessage(),kafkaEntity.getName(),kafkaEntity.getCreate_time());
            }
        });
        return row;
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        String[] names = new String[] {"id" , "message","name","create_date"};
        TypeInformation[] types = new TypeInformation[] {Types.STRING(), Types.STRING(),Types.STRING(),Types.LONG()};
        return Types.ROW(names, types);
    }

    @Override
    public TableSchema getTableSchema() {
        return null;
    }
}
