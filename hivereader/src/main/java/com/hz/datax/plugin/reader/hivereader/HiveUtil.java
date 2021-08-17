package com.hz.datax.plugin.reader.hivereader;

import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @Author liansongye
 * @create 2021/8/13 5:16 下午
 */
public class HiveUtil {
    private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static Connection con = null;

    public static Connection getConnect(String url, String db){
        try {
            Class.forName(driverName);
            url = url.endsWith("/") ? url : url + "/";
            url = StringUtils.isNotBlank(db) ? url + db : url;
            con = DriverManager.getConnection(url,"","");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return con;
    }

    public static boolean executeDdl(String sql) throws SQLException {
        return con.createStatement().execute(sql);
    }

}
