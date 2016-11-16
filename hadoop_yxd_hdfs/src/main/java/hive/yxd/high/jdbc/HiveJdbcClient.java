package hive.yxd.high.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by XuanYu on 2016/11/13.
 */
public class HiveJdbcClient {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws Exception{

        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Connection con = DriverManager
                .getConnection(
                        "jdbc:hive2://hadoop1:10000/metastore",
                        "root",
                        "root"
                );
        Statement stmt = con.createStatement();
        String tableName = "urlinfo u";

        String sql = "SELECT u.ip,u.url from  "  + tableName ;

        ResultSet res = stmt.executeQuery(sql);

        while (res.next()) {
            System.out.println(
            		
                    res.getString(1) + "\t" +
                    res.getString(2) + "\t" +"\n");
        }

        res.close();
        stmt.close();
        con.close();
    }
}
