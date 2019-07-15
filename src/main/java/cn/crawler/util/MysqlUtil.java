package cn.crawler.util;

import cn.crawler.mft_first.Agent;
import cn.crawler.mft_first.MallUserLogin;
import cn.crawler.mft_seconed.KafkaEntity;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by liuliang on 2019/5/17.
 */
public class MysqlUtil {


    PreparedStatement ps;
    private Connection connection;


    public void insertKafkaEntity(KafkaEntity kafkaEntity) throws Exception {
        System.out.println("正准备插入数据："+kafkaEntity.toString());
        connection = getConnection();
        String sql = "insert into kafka_entity values ( '"+kafkaEntity.getId()+"' ,'"+kafkaEntity.getMessage()+"')";
        ps = this.connection.prepareStatement(sql);
        ps.execute();
        close();
    }


    public List<MallUserLogin> getLogins(String userId) throws Exception {
        connection = getConnection();
        String sql = "SELECT * FROM `m_mall_user`.user_login_info WHERE user_id = ?  order by login_time desc ";
        ps = this.connection.prepareStatement(sql);
        ps.setString(1, userId);
        ResultSet resultSet = ps.executeQuery();

        List<MallUserLogin> list = new ArrayList<>();

        while (resultSet.next()) {
            MallUserLogin userLogin = new MallUserLogin();
            Date loginTime = resultSet.getDate("login_time");
            String id = resultSet.getString("id");
            String userId1 = resultSet.getString("user_id");
            String address = resultSet.getString("address");
            String ip = resultSet.getString("ip");
            userLogin.setId(id);
            userLogin.setAddress(address);
            userLogin.setIp(ip);
            userLogin.setLoginTime(loginTime);
            userLogin.setUserId(userId1);
            list.add(userLogin);
        }
        close();
        return list;
    }


    public List<Agent> getAgents() throws Exception {
        connection = getConnection();
        String sql = "SELECT id,parent_id parentId FROM `m_mall_agent`.`mall_agent` ";
        ps = this.connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        List<Agent> list = new ArrayList<>();

        while (resultSet.next()) {
            list.add(Agent.builder()
                    .id(resultSet.getString("id"))
                    .parentId(resultSet.getString("parentId"))
                    .build());
        }
        close();
        return list;
    }


    /**
     * 程序执行完毕就可以进行，关闭连接和释放资源的动作了
     *
     * @throws Exception
     */
    public void close() throws Exception {
        if (connection != null) { //关闭连接和释放资源
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }


    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://www.iamcrawler.cn:3306/crawler_flink?useUnicode=true&characterEncoding=UTF-8", "root", "123456");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }


}
