package cn.crawler.util;

import cn.crawler.mft_first.MallUserLogin;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * Created by liuliang
 * on 2019/6/12
 */
public class RabbitMqSend {

    //队列名称
    public static final String QUEUE = "test_user_login";


    public static void main(String[] args) throws IOException, TimeoutException {

        Connection connection = RabbitMqUtil.getConnection();
        //从连接中获取一个通道
        Channel channel = connection.createChannel();
        //声明队列
//        channel.queueDeclare(QUEUE, false, false, false, null);

        for (int i = 0; i < 10; i++) {
            MallUserLogin userLogin = new MallUserLogin();
            userLogin.setIp("1234");
            if (i % 2 == 0) {
                userLogin.setAddress("上海");
            } else {
                userLogin.setAddress("北京");
            }
            userLogin.setEmail("iamcrawler@sina.com");
//            userLogin.setId(Math.random()*100000+"");
            userLogin.setLoginTime(new Date());
            userLogin.setUserId(i + "");
            userLogin.setId("" + i);
            userLogin.setAge(i);
            //发送消息
            channel.basicPublish("", QUEUE, null, GsonUtil.toJSONBytes(userLogin));
            System.out.println("success:{}" + userLogin);
        }
        channel.close();
        connection.close();

    }

}
