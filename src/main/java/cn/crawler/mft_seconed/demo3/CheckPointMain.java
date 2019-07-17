package cn.crawler.mft_seconed.demo3;




import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by liuliang
 * on 2019/7/15
 */
public class CheckPointMain {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new FsStateBackend("file:///checkpoint/"));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointInterval(1000);

        DataStream<Tuple4<Integer,String,String,Integer>> data = env.setParallelism(1).addSource(new AutoSourceWithCp());
        env.setParallelism(1);
        data.keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .apply(new WindowStatisticWithChk())
                .print();

        env.execute();
    }

}
