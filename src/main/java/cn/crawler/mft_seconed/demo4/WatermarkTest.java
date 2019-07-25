package cn.crawler.mft_seconed.demo4;


import cn.crawler.mft_seconed.KafkaEntity;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.19.141.60:31090");
        properties.setProperty("group.id", "crm_stream_window");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        DataStream<String> stream =
                env.addSource(new FlinkKafkaConsumer011<>("test-demo13", new SimpleStringSchema(), properties));
        env.setParallelism(1);
        DataStream<Tuple3<String, Long, Integer>> inputMap = stream.map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
            private static final long serialVersionUID = -8812094804806854937L;

            @Override
            public Tuple3<String, Long, Integer> map(String value) throws Exception {
                KafkaEntity kafkaEntity = JSON.parseObject(value, KafkaEntity.class);
                return new Tuple3(kafkaEntity.getName(), kafkaEntity.getCreate_time(), kafkaEntity.getId());
            }
        });
        DataStream<Tuple3<String, Long, Integer>> watermark =
                inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Long, Integer>>() {

                    private static final long serialVersionUID = 8252616297345284790L;
                    Long currentMaxTimestamp = 0L;
                    Long maxOutOfOrderness = 2000L;//最大允许的乱序时间是2s
                    Watermark watermark = null;
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                        return watermark;
                    }

                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Integer> element, long previousElementTimestamp) {
                        Long timestamp = element.f1;
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        System.out.println("timestamp : " + element.f1 + "|" + format.format(element.f1) + " currentMaxTimestamp : " + currentMaxTimestamp + "|" + format.format(currentMaxTimestamp) + "," + " watermark : " + watermark.getTimestamp() + "|" + format.format(watermark.getTimestamp()));
                        return timestamp;
                    }
                });

        OutputTag<Tuple3<String, Long, Integer>> lateOutputTag = new OutputTag<Tuple3<String, Long, Integer>>("late-data") {
            private static final long serialVersionUID = -1552769100986888698L;
        };

        SingleOutputStreamOperator<String> resultStream = watermark
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .trigger(new Trigger<Tuple3<String, Long, Integer>, TimeWindow>() {
                    private static final long serialVersionUID = 2742133264310093792L;
                    ValueStateDescriptor<Integer> sumStateDescriptor = new ValueStateDescriptor<Integer>("sum", Integer.class);

                    @Override
                    public TriggerResult onElement(Tuple3<String, Long, Integer> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        ValueState<Integer> sumState = ctx.getPartitionedState(sumStateDescriptor);
                        if (null == sumState.value()) {
                            sumState.update(0);
                        }
                        sumState.update(element.f2 + sumState.value());
                        System.out.println(sumState.value());
//                        if (sumState.value() >= 2) {
                            //这里可以选择手动处理状态
                            //  默认的trigger发送是TriggerResult.FIRE 不会清除窗口数据
//                            return TriggerResult.FIRE_AND_PURGE;
                                return TriggerResult.FIRE_AND_PURGE;
//                        }
//                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        System.out.println("清理窗口状态 | 窗口内保存值为" + ctx.getPartitionedState(sumStateDescriptor).value());
                        ctx.getPartitionedState(sumStateDescriptor).clear();
                    }
                })
                //如果使用allowedLateness会有重复计算的效果
                //默认的trigger情况下
                // 在event time>window_end_time+watermark+allowedLateness时会触发窗口的clear
                // 后续数据如果属于该窗口而且数据的event_time>watermark-allowedLateness 会触发重新计算
                //
                //在使用自定义的trigger情况下
                //同一个窗口内只要满足要求可以不停的触发窗口数据往下流
                //在event time>window_end_time+watermark+allowedLateness时会触发窗口clear
                //后续数据如果属于该窗口而且数据的event_time>watermark-allowedLateness 会触发重新计算
                //
                //窗口状态的clear只和时间有关与是否自定义trigger无关
                .allowedLateness(Time.seconds(3))
                .sideOutputLateData(lateOutputTag)
                .apply(new WindowFunction<Tuple3<String, Long, Integer>, String, Tuple, TimeWindow>() {
                    private static final long serialVersionUID = 7813420265419629362L;

                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<String, Long, Integer>> input, Collector<String> out) throws Exception {
                        for (Tuple3<String, Long, Integer> stringLongTuple2 : input) {
                            System.out.println(stringLongTuple2.f1);
                        }
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        out.collect("window  " + format.format(window.getStart()) + "   window  " + format.format(window.getEnd()));
                        System.out.println("-------------------------");
                    }
                });

        resultStream.print();
        resultStream.getSideOutput(lateOutputTag).print();
        env.execute("window test");

    }
}
