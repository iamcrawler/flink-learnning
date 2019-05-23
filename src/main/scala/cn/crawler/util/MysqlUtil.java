package cn.crawler.util;

import cn.crawler.mft.MallUserLogin;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by liuliang on 2019/5/17.
 */
public class MysqlUtil {


    PreparedStatement ps;
    private Connection connection;



    public List<MallUserLogin> getLogins(String userId) throws Exception {
        connection = getConnection();
        String sql = "SELECT * FROM `m_mall_user`.user_login_info WHERE user_id = ?  order by login_time desc ";
        ps = this.connection.prepareStatement(sql);
        ps.setString(1,userId);
        ResultSet resultSet = ps.executeQuery();

        List<MallUserLogin> list = new ArrayList<>();

        while (resultSet.next()) {
            MallUserLogin userLogin = new MallUserLogin(
//                    resultSet.getInt("id"),
//                    resultSet.getString("name").trim(),
//                    resultSet.getString("password").trim(),
//                    resultSet.getInt("age")
            );
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
            con = DriverManager.getConnection("jdbc:mysql://139.224.168.174:3332/m_mall_user?useUnicode=true&characterEncoding=UTF-8", "dev_client_001", "meifute@123");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return con;
    }


}
