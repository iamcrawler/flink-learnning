package cn.crawler.hello;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

/**
 * Created by liuliang
 * on 2019/5/27
 */
public class HelloWorld {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();





        //key by
        //分组，可以理解为group by
        //将DataStream→KeyedStream  后面基于KeyedStream 的操作，都是组内操作
        //但是要注意，1.不能重写hashCode方法 2.是一个数组
        //key by() 使用的是散列分区实现的，指定的建有不同的方法
//        env.fromElements(Tuple2.of(1,2),Tuple2.of(2,4),Tuple2.of(1,3),Tuple2.of(1,4),Tuple2.of(2,3))
//                .keyBy(0)
//                .map((MapFunction<Tuple2<Integer, Integer>, String>) tuple2->"key:"+tuple2.f0+" value:"+tuple2.f1)
//                .print();
//        env.execute("hello world");


        //reduce
        //reduce表示将数据合并成一个新的数据，返回单个的结果值
        //reduce方法不能直接应用于SingleOutputStreamOperator对象，
        // 也好理解，因为这个对象是个无限的流，对无限的数据做合并，没有任何意义哈！
        //可用于 keyby() window/timeWindow 处
//        env.fromElements(Tuple2.of(1,2),Tuple2.of(2,4),Tuple2.of(1,3),Tuple2.of(1,4),Tuple2.of(2,3))
//                .keyBy(0)
//                .reduce((ReduceFunction<Tuple2<Integer, Integer>>) (t2,t1)->Tuple2.of(t2.f0,t2.f1+t1.f1))
//                .print();
//        env.execute("hello world");


        //聚合 KeyedStream→DataStream
//        env.fromElements(Tuple2.of(1,2),Tuple2.of(2,4),Tuple2.of(1,3),Tuple2.of(1,4),Tuple2.of(2,3))
//                .keyBy(0)
//                .sum(0)
//                .addSink(new PrintSinkFunction<>());
//
//        env.execute("hello world");

        //aggregate  HotItems里面有现成例子



    }
}
