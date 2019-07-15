package cn.crawler.util;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;

/**
 * Created by liuliang
 * on 2019/6/12
 */
public class RabbitMqUtil {


    public static Connection getConnection() {
        try {
            Connection connection = null;
            //定义一个连接工厂
            ConnectionFactory factory = new ConnectionFactory();
            //设置服务端地址（域名地址/ip）
            factory.setHost("www.iamcrawler.cn");
            //设置服务器端口号
            factory.setPort(5672);
            //设置虚拟主机(相当于数据库中的库)
            factory.setVirtualHost("/");
            //设置用户名
            factory.setUsername("liuliang");
            //设置密码
            factory.setPassword("liuliang");
            connection = factory.newConnection();
            return connection;
        } catch (Exception e) {
            return null;
        }
    }


}
