package cn.crawler.mft;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by liuliang on 2019/5/17.
 */
public class SourceFromMySQL extends RichSourceFunction<MallUserLogin> {

    PreparedStatement ps;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接。
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String userId = parameters.getString("userId", "1083250778777649152");
        connection = getConnection();
        String sql = "SELECT * FROM `m_mall_user`.user_login_info WHERE user_id = ? ";
        ps.setString(1,userId);
        ps = this.connection.prepareStatement(sql);
    }

    /**
     * 程序执行完毕就可以进行，关闭连接和释放资源的动作了
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) { //关闭连接和释放资源
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * DataStream 调用一次 run() 方法用来获取数据
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<MallUserLogin> ctx) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            MallUserLogin userLogin = new MallUserLogin(
//                    resultSet.getInt("id"),
//                    resultSet.getString("name").trim(),
//                    resultSet.getString("password").trim(),
//                    resultSet.getInt("age")
            );
            ctx.collect(userLogin);
        }
    }

    @Override
    public void cancel() {
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