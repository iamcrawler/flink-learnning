package cn.crawler.mft;

import cn.crawler.util.GsonUtil;
import cn.crawler.util.MysqlUtil;
import cn.crawler.util.RedisUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by liuliang
 * on 2019/5/28
 *
 * 处理mft 测试环境的代理树，每10s刷新一次
 *
 */
public class AgentTree {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSink<Agent> sink = env.addSource(new SourceFromMySQL())
                .addSink(new RichSinkFunction<Agent>() {
            @Override
            public void invoke(Agent value, Context context) throws Exception {
                RedisUtil redisUtil = new RedisUtil();
                redisUtil.setValue(value.getId(), value.getParentId());
            }
        }).setParallelism(1);
        env.execute("flink test");
    }
}
