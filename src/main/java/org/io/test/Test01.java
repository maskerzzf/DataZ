package org.io.test;

import org.io.common.data.Configuration;
import org.io.common.data.TransitData;
import org.io.common.mysql.writer.MysqlWriter;
import org.io.common.sqlserver.reader.SqlserverReader;

import java.util.HashMap;

public class Test01 {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        HashMap<String, String> source = new HashMap<>();
        source.put("url","jdbc:sqlserver://localhost:1433;databaseName=school;trustServerCertificate=true");
        source.put("user","sa");
        source.put("password","root");
        source.put("timeout","");
        HashMap<String, String> target = new HashMap<>();
        target.put("url","jdbc:mysql://localhost:3306/school1");
        target.put("user","root");
        target.put("password","root");
        target.put("timeout","");
        configuration.setSourceDbInfo(source);
        configuration.setTargetDbInfo(target);

        SqlserverReader sqlserverReader = new SqlserverReader(configuration);
        sqlserverReader.init();
        TransitData post = sqlserverReader.post();
        sqlserverReader.destroy();
        MysqlWriter mysqlWriter = new MysqlWriter(configuration,post);
        mysqlWriter.init();
        mysqlWriter.post();
        mysqlWriter.destroy();
    }
}
