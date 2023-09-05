package org.io.test;

import java.sql.SQLException;

public class TestGetTable {

    public static void main(String[] args) throws SQLException {

//        Mysql2Mysql mysql2mysql = new Mysql2Mysql();
//        mysql2mysql.run();
          Sqlserver2MySql sqlserver2MySql = new Sqlserver2MySql();
          sqlserver2MySql.run();

    }


}
