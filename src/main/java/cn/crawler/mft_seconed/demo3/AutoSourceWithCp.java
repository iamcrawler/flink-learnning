package cn.crawler.mft_seconed.demo3;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by liuliang
 * on 2019/7/15
 */
public class AutoSourceWithCp extends RichSourceFunction<Tuple4<Integer,String,String,Integer>> implements ListCheckpointed<UserState> {
    private int count = 0;
    private boolean is_running = true;

    @Override
    public void run(SourceContext sourceContext) throws Exception {

        while(is_running){
            for (int i = 0; i < 10; i++) {
                sourceContext.collect(Tuple4.of(1, "hello-" + count, "alphabet", count));
                count++;
            }
            System.out.println("source:"+count);
            Thread.sleep(2000);

            if(count>100){
                System.out.println("准备异常啦.....");
                throw new Exception("exception made by ourself!");
            }
        }
    }

    @Override
    public void cancel() {
        is_running = false;
    }

    @Override
    public List<UserState> snapshotState(long l, long l1) throws Exception {
        List<UserState> listState= new ArrayList<>();
        UserState state = new UserState(count);
        listState.add(state);
        System.out.println(System.currentTimeMillis()+" #############  check point :"+listState.get(0).getCount());
        return listState;
    }

    @Override
    public void restoreState(List<UserState> list) throws Exception {

        count = list.get(0).getCount();
        System.out.println("AutoSourceWithCp restoreState:"+count);

    }
}
