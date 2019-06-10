package cn.crawler.hello;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liuliang on 2019/5/7.
 */
public class StreamingWindowWordCountJava {


    private Logger logger = LoggerFactory.getLogger(StreamingWindowWordCountJava.class);

    public static void main(String[] args) throws Exception {
        //定义socket的端口号
//        int port;
//        try{
//            ParameterTool parameterTool = ParameterTool.fromArgs(args);
//            port = parameterTool.getInt("port");
//        }catch (Exception e){
//            System.out.println("没有指定port参数，使用默认值6100");
//            port = 6100;
//        }
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //连接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream("47.106.77.78", 8083, "\n");
        // 本文填写的是远程linux ip，在远程linux上需要执行：nc -l 6100命令
        //计算数据
        DataStream<WordWithCount> windowCount = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String[] splits = value.split("\\s");
                for (String word:splits) {
                    out.collect(new WordWithCount(word,1L));
                }
            }
        })//打平操作，把每行的单词转为<word,count>类型的数据
                //针对相同的word数据进行分组
                .keyBy("word")
                //指定计算数据的窗口大小和滑动窗口大小
                .timeWindow(Time.seconds(5),Time.seconds(1))
                .sum("count");
        //把数据打印到控制台,使用一个并行度
        windowCount.print().setParallelism(1);
        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("streaming word count");
    }

    /**
     * 主要为了存储单词以及单词出现的次数
     */
    public static class WordWithCount{
        public String word;
        public long count;
        public WordWithCount(){}
        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
