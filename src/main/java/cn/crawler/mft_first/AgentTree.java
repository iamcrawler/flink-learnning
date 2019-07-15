package cn.crawler.mft_first;

import cn.crawler.util.RedisUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Created by liuliang
 * on 2019/5/28
 * <p>
 * 处理mft 测试环境的代理树，每10s刷新一次
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
