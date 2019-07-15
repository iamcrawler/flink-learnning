package cn.crawler.mft_seconed.demo2;

import cn.crawler.mft_seconed.KafkaEntity;
import cn.crawler.util.MysqlUtil;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Created by liuliang
 * on 2019/7/15
 */
public class SinkKafkaEntity2Mysql extends RichSinkFunction<KafkaEntity> {
    /**
     * 每条数据得插入都要掉一次invoke方法
     * @param kafkaEntity
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(KafkaEntity kafkaEntity, Context context) throws Exception {
        MysqlUtil mysqlUtil = new MysqlUtil();
        mysqlUtil.insertKafkaEntity(kafkaEntity);
    }
}
