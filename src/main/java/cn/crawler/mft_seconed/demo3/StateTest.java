package cn.crawler.mft_seconed.demo3;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.temporal.TemporalUnit;

/**
 * Created by liuliang
 * on 2019/7/15
 */
public class StateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1).fromElements(Tuple2.of(1L,1L),Tuple2.of(2L,2L),Tuple2.of(3L,2L),Tuple2.of(4L,3L))
                .keyBy(0).flatMap(new RichFlatMapFunction<Tuple2<Long,Long>, Object>() {
            private transient ValueState<Tuple2<Long,Long>> sum;
            @Override
            public void flatMap(Tuple2<Long, Long> longLongTuple2, Collector<Object> collector) throws Exception {

                Tuple2<Long,Long> curSum = sum.value();
                curSum.f0+=1;
                curSum.f1+=longLongTuple2.f1;
                System.out.println("+");
                sum.update(curSum);
                if(curSum.f0>0){
                    System.out.println("-");
                    collector.collect(Tuple2.of(curSum.f0,curSum.f1));
                    sum.clear();
                }
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Tuple2<Long,Long>> descriptor = new ValueStateDescriptor<Tuple2<Long, Long>>("avg", TypeInformation.of(
                        new TypeHint<Tuple2<Long, Long>>() {}), Tuple2.of(0L,0L));
                sum = getRuntimeContext().getState(descriptor);
            }
        }).print();
        env.execute();

    }
}
