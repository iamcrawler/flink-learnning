package cn.crawler.mft_seconed.demo4;

import cn.crawler.mft_seconed.KafkaEntity;
import cn.crawler.mft_seconed.demo3.AutoSourceWithCp;
import cn.crawler.mft_seconed.source.KafkaSource;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;


import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 水印的作用：告知算子之后不会有小于或等于水印时间戳的事件。
 */
public class EventTimeGenerateTimestamp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //按照事件事件处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //1S执行一次checkpoint
        env.enableCheckpointing(1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime); // default


        //EXACTLY_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.setParallelism(1);

        System.out.println("123");
        Thread.sleep(100);
        System.out.println("456");

        DataStreamSource<String> kafkaSource = env.addSource(KafkaSource.getKafkaSource("demo9"));
        SingleOutputStreamOperator<KafkaEntity> map = kafkaSource.map(new MapFunction<String, KafkaEntity>() {
            private static final long serialVersionUID = 1471936326697828381L;
            @Override
            public KafkaEntity map(String value) {
                KafkaEntity kafkaEntity = JSON.parseObject(value, KafkaEntity.class);
                return kafkaEntity;
            }
        });

        //生成水印
        SingleOutputStreamOperator<KafkaEntity> waterMarkStream = map.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<KafkaEntity>() {


            final Long maxOutOfOrder = 1L;// 最大允许的乱序时间是100ms
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            /**
             * 定义如何提取timestamp
             * @param element
             * @param previousElementTimestamp
             * @return
             */
            @Override
            public long extractTimestamp(KafkaEntity element, long previousElementTimestamp) {
                long timestamp = element.getCreate_time();

//                System.out.println("key:" + element.getName() + ",eventtime:[" + element.getCreate_time() + "|" + sdf.format(element.getCreate_time()) + "],currentMaxTimestamp:[" + currentMaxTimestamp + "|" +
//                        sdf.format(currentMaxTimestamp) + "],watermark:[" + getCurrentWatermark().getTimestamp() + "|" + sdf.format(getCurrentWatermark().getTimestamp()) + "]");
                return timestamp;
            }

            /**
             * 定义生成watermark的逻辑
             * @return
             */
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(System.currentTimeMillis()- maxOutOfOrder);
            }
        });


        waterMarkStream.print();

//        SingleOutputStreamOperator window = waterMarkStream.keyBy("id")
//                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
//                .apply(
//                        new WindowFunction<KafkaEntity, String, Tuple, TimeWindow>() {
//                            /**
//                             * 对window内的数据进行排序，保证数据的顺序
//                             * @param tuple
//                             * @param window
//                             * @param input
//                             * @param out
//                             * @throws Exception
//                             */
//                            @Override
//                            public void apply(Tuple tuple, TimeWindow window, Iterable<KafkaEntity> input, Collector<String> out) throws Exception {
//                                String key = tuple.toString();
//                                List<Long> arrarList = new ArrayList<Long>();
//                                Iterator<KafkaEntity> it = input.iterator();
//                                while (it.hasNext()) {
//                                    KafkaEntity next = it.next();
//                                    arrarList.add(next.getCreate_time());
//                                }
//                                Collections.sort(arrarList);
//                                System.out.println("list:"+arrarList);
//                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//                                String result = key + "," + arrarList.size() + "," + sdf.format(arrarList.get(0)) + "," + sdf.format(arrarList.get(arrarList.size() - 1))
//                                        + "," + sdf.format(window.getStart()) + "," + sdf.format(window.getEnd());
//                                out.collect(result);
//                            }
//                        }
//
////                        new WindowFunction() {
////                    @Override
////                    public void apply(Object o, Window window, Iterable input, Collector out) throws Exception {
////                        Integer id = Integer.parseInt(o.toString());
////                        List<Integer> list = new ArrayList<Integer>();
////                        Iterator<KafkaEntity> iterator = input.iterator();
////                        while (iterator.hasNext()) {
////                            KafkaEntity kafkaEntity = iterator.next();
////                            list.add(kafkaEntity.getId());
////                        }
////                        Collections.sort(list);
////                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
////
////                        String result = id + "," + list.size() + "," + sdf.format(list.get(0)) + "," + sdf.format(list.get(list.size() - 1))
////                                + "," + sdf.format(window.maxTimestamp());
////                        out.collect(result);
////
////                    }
////                }
//                );
//
//        window.print();

        env.execute("Event Time");


    }
}
