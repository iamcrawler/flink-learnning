package cn.crawler.mft_seconed.demo3;




import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 基于上面的提问，自定义一个state实现checkpoint接口
 * Created by liuliang
 * on 2019/7/15
 */
public class CheckPointMain {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new FsStateBackend("file:///Users/liuliang/Documents/others/flinkdata/state_checkpoint"));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointInterval(1000);

        /**
         * 说明:
         * 因为需要实现ListCheckpoint接口，所以source和windows处理代码，单独写成了JAVA类的形似，
         * 实现逻辑和验证方法跟manage state相似，但是在如下代码中，Source和Window都实现了ListCheckpoint接口，
         * 也就是说，Source抛出异常的时候，Source和Window都将可以从checkpoint中获取历史状态，从而达到不丢失状态的能力。
         */
        DataStream<Tuple4<Integer,String,String,Integer>> data = env.setParallelism(1).addSource(new AutoSourceWithCp());
        env.setParallelism(1);
        data.keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .apply(new WindowStatisticWithChk())
                .print();

        env.execute();
    }

}
