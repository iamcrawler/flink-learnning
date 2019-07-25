package cn.crawler.mft_seconed.demo4;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class Demo4Source extends RichSourceFunction<Tuple4<Integer,String,String,Integer>> {



    @Override
    public void run(SourceContext<Tuple4<Integer, String, String, Integer>> ctx) {



    }

    @Override
    public void cancel() {

    }
}
