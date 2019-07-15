package cn.crawler.wiki;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * Created by liuliang on 2019/5/7.
 */
public class WikiDemo {


    public static void main(String[] args) throws Exception {

        //获取运行环境
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置执行参数，创建外部系统的读取源
        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
                .keyBy(new KeySelector<WikipediaEditEvent, String>() {
                    public String getKey(WikipediaEditEvent event) {
                        return event.getUser();  //按照用户名组织数据
                    }
                });

        DataStream<Tuple2<String, Long>> result = keyedEdits
                .timeWindow(Time.seconds(60))
                .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {
                        acc.f0 = event.getUser();
                        acc.f1 += event.getByteDiff();
                        return acc;
                    }
                });

        result.print();  // 也可以将result写入文件或者kafka

        see.execute();

    }


}
