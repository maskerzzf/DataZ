package org.io.common.enums;


/**
 * 根据不同的数据库加载不同的驱动
 */
public enum DatabaseTypeEnum {
    MYSQL("mysql","com.mysql.jdbc.Driver"),
    MYSQL8("mysql8","com.mysql.cj.jdbc.Driver"),
    SQLSERVER("sqlserver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    ;
    private String typeName;
    private String driverClassName;

    DatabaseTypeEnum(String typeName, String driverClassName) {
        this.typeName = typeName;
        this.driverClassName = driverClassName;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }
}
