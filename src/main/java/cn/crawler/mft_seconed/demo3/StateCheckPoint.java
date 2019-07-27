package cn.crawler.mft_seconed.demo3;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by liuliang
 * on 2019/7/15
 */
public class StateCheckPoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();

        //打开并设置checkpoint
        // 1.设置checkpoint目录，这里我用的是本地路径，记得本地路径要file://开头
        // 2.设置checkpoint类型，at lease onece or EXACTLY_ONCE
        // 3.设置间隔时间，同时打开checkpoint功能
        //
        env.setStateBackend(new FsStateBackend("file:///Users/liuliang/Documents/others/flinkdata/state_checkpoint"));

//        env.setStateBackend(new FsStateBackend("file://D:\\softs\\flink\\state"));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(1000);


        //添加source 每个2s 发送10条数据，key=1，达到100条时候抛出异常

        //source发送记录到达100抛出异常
        //source抛出异常之后，count发送统计数丢失，重新从0开始
        //windows函数，重启后调用open函数，获取state数据，处理记录数从checkpoint中获取恢复，所以从100开始
        //总结：source没有使用manage state状态丢失，windows使用manage state，异常状态不丢失
        //问: 1. state.value()在open（）方法中调用的时候，会抛出null异常，而在apply中使用就不会抛出异常。为什么？
        //    2. 为什么source里面没有open方法？source想使用state该怎么操作？
        env.addSource(new SourceFunction<Tuple3<Integer,String,Integer>>() {
            private Boolean isRunning = true;
            private int count = 0;

            @Override
            public void run(SourceContext<Tuple3<Integer, String, Integer>> sourceContext) throws Exception {
                while(isRunning){

                    for (int i = 0; i < 10; i++) {
                        sourceContext.collect(Tuple3.of(1,"ahah",count));
                        count++;
                    }
                    if(count>100){
                        System.out.println("err_________________");
                        throw new Exception("123");
                    }
                    System.out.println("source:"+count);
                    Thread.sleep(2000);
                }

            }

            @Override
            public void cancel() {

            }
        }).keyBy(0)

                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))

                //窗口函数，比如是richwindowsfunction 否侧无法使用manage state
                .apply(new RichWindowFunction<Tuple3<Integer,String,Integer>, Integer, Tuple, TimeWindow>() {
                    private transient ValueState<Integer> state;
                    private int count = 0;
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<Integer, String, Integer>> iterable, Collector<Integer> collector) throws Exception {
                        //从state中获取值
                        count=state.value();
                        for(Tuple3<Integer, String, Integer> item : iterable){
                            count++;
                        }
                        //更新state值
                        state.update(count);
                        System.out.println("windows:"+tuple.toString()+"  "+count+"   state count:"+state.value());
                        collector.collect(count);
                    }


                    //获取state
                    @Override
                    public void open(Configuration parameters) throws Exception {
//                        state.value();   空指针
                        System.out.println("##open");
                        ValueStateDescriptor<Integer> descriptor =
                                new ValueStateDescriptor<Integer>(
                                        "average", // the state name
                                        TypeInformation.of(new TypeHint<Integer>() {}), // type information
                                        0);
                        state = getRuntimeContext().getState(descriptor);




                    }
                }).print();
        env.execute();

    }
}
