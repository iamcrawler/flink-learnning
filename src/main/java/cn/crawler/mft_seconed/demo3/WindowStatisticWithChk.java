package cn.crawler.mft_seconed.demo3;


import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;


import java.util.ArrayList;
import java.util.List;


import java.util.ArrayList;

/**
 * Created by liuliang
 * on 2019/7/15
 */
public class WindowStatisticWithChk implements WindowFunction<Tuple4<Integer,String,String,Integer>,Integer,Tuple,TimeWindow> ,ListCheckpointed<UserState> {
    private int total = 0;
    @Override
    public List<UserState> snapshotState(long l, long l1) throws Exception {

        List<UserState> listState= new ArrayList<>();
        UserState state = new UserState(total);
        listState.add(state);
        return listState;
    }

    @Override
    public void restoreState(List<UserState> list) throws Exception {
        total = list.get(0).getCount();
    }

    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple4<Integer, String, String, Integer>> iterable, Collector<Integer> collector) throws Exception {
        int count  =  0;
        for(Tuple4<Integer, String, String, Integer> data : iterable){
            count++;
            System.out.println("apply key"+tuple.toString()+" count:"+data.f3+"     "+data.f0);
        }
        total = total+count;
        System.out.println("windows  total:"+total+"  count:"+count+"   ");
        collector.collect(count);
    }


}
