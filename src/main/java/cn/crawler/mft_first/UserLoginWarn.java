package cn.crawler.mft_first;

import cn.crawler.util.GsonUtil;
import cn.crawler.util.MailUtil;
import cn.crawler.util.MysqlUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by liuliang on 2019/5/17.
 * 异地登录警告
 */
@Slf4j
public class UserLoginWarn {


    public static final String QUEUE = "test_user_login";


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //下面这些写死的参数可以放在配置文件中，然后通过 parameterTool 获取
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig
                .Builder().setHost("www.iamcrawler.cn").setVirtualHost("/")
                .setPort(5672).setUserName("liuliang").setPassword("liuliang")
                .build();

        SingleOutputStreamOperator<MallUserLogin> test = env.addSource(new RMQSource<>(connectionConfig,
                QUEUE,
                true,
                new SimpleStringSchema()))
                .setParallelism(1)
                .map(string -> GsonUtil.fromJson(string, MallUserLogin.class))
                .setParallelism(1);


//        test.print();

//         map 将ip + AAAAA...
//        SingleOutputStreamOperator<MallUserLogin> map = test.map(new MapFunction<MallUserLogin, MallUserLogin>() {
//////            @Override
//////            public MallUserLogin map(MallUserLogin mallUserLogin) throws Exception {
//////                mallUserLogin.setIp(mallUserLogin.getIp() + "AAAAAAAAA");
//////                return mallUserLogin;
//////            }
//////        });
//////        map.print();

        //FlatMap 采用一条记录并输出零个，一个或多个记录。
//        SingleOutputStreamOperator<MallUserLogin> flatMap = test.flatMap(new FlatMapFunction<MallUserLogin, MallUserLogin>() {
//            @Override
//            public void flatMap(MallUserLogin mallUserLogin, Collector<MallUserLogin> out) throws Exception {
//
//                if ("上海".equals(mallUserLogin.getAddress())) {
//                    out.collect(mallUserLogin);
//                }
//            }
//        });
//
//        flatMap.print();

        //Filter 很简单...省略吧

        //key by 在逻辑上对key进行流分区，内部使用hash对流分区，返回KeyedDataStream 数据流
//        KeyedStream<MallUserLogin, String> keyBy = test.keyBy(new KeySelector<MallUserLogin, String>() {
////            @Override
////            public String getKey(MallUserLogin mallUserLogin) throws Exception {
////                return mallUserLogin.getAddress();
////            }
////        });
////        keyBy.print();


        //Reduce 返回单个的结果值，并且reduce操作处理每一个元素总是创建一个新值。
        //常用的方法有 average, sum, min, max, count，使用 reduce 方法都可实现。
        SingleOutputStreamOperator<MallUserLogin> reduce = test.keyBy(new KeySelector<MallUserLogin, String>() {
            @Override
            public String getKey(MallUserLogin mallUserLogin) throws Exception {
                return mallUserLogin.getAddress();
            }
        }).reduce(new ReduceFunction<MallUserLogin>() {
            @Override
            public MallUserLogin reduce(MallUserLogin t0, MallUserLogin t1) throws Exception {
                MallUserLogin mallUserLogin = new MallUserLogin();
                mallUserLogin.setAge(t0.getAge() + t1.getAge());
                return mallUserLogin;
            }
        });
        reduce.print();

        //可以看到，reduce是基于keyby的基础上操作的...


        //.....

//        test.addSink(new PrintSinkFunction<>());
//        test.addSink(new SinkFunction<MallUserLogin>() {
//            @Override
//            public void invoke(MallUserLogin value, Context context) throws Exception {
//                //...
//            }
//        });

        test.timeWindowAll(Time.seconds(3)).apply(new AllWindowFunction<MallUserLogin, List<MallUserLogin>, TimeWindow>() {
                                                      @Override
                                                      public void apply(TimeWindow timeWindow, Iterable<MallUserLogin> iterable, Collector<List<MallUserLogin>> collector) throws Exception {
                                                          ArrayList<MallUserLogin> logins = Lists.newArrayList(iterable);
                                                          if (logins.size() > 0) {
                                                              System.out.println("3s 内 一共有：" + logins.size() + " 条数据...准备分析");

                                                              for (MallUserLogin login : logins) {
                                                                  MysqlUtil mysqlUtil = new MysqlUtil();
                                                                  List<MallUserLogin> list = mysqlUtil.getLogins(login.getUserId());
                                                                  System.out.println(list.size() + " 次");
                                                                  list = list.stream().filter(
                                                                          l -> {
                                                                              return !l.getId().equals(login.getId());
                                                                          }
                                                                  ).collect(Collectors.toList());

                                                                  System.out.println("除去本次，总共登陆过：" + list.size() + " 次");
                                                                  System.out.println(list.toString());
                                                                  List<String> collect = list.stream().map(MallUserLogin::getAddress).collect(Collectors.toList());
                                                                  if (!CollectionUtils.isEmpty(list) && !collect.contains(login.getAddress())) {
                                                                      System.out.println("异地登陆...准备发送警告");
                                                                      MailUtil.AuthMailSSL(StringUtils.isEmpty(login.getEmail()) ? "iamcrawler@sina.com" : login.getEmail(), login.getAddress());
                                                                      System.out.println("警告邮件已发送...");

                                                                  } else {
                                                                      System.out.println("正常登陆...");
                                                                  }
                                                              }
                                                          } else {
                                                              System.out.println("走了else........");
                                                          }
                                                      }
                                                  }
        ).setParallelism(1);
//        test.addSink(new PrintSinkFunction<>());
//        configuration.setString("id",test.map());
//        DataStreamSource<MallUserLogin> source = env.addSource(new SourceFromMySQL());

        //如果想保证 exactly-once 或 at-least-once 需要把 checkpoint 开启
//        env.enableCheckpointing(10000);
        env.execute("flink test for connectors rabbitmq");
    }


}
