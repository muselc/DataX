package com.hz.datax.plugin.reader.hivereader;


public class Key {
    /**
     * 1.必选:hiveSql,defaultFS,jdbc
     * 2.可选(有缺省值):
     * 			tempDb(default)
     *          tempDbPath(/user/hive/warehouse/)
     *          fieldDelimiter(\u0001)
     * 3.可选(无缺省值):
     * */


    public final static String DEFAULT_FS = "defaultFS";
    public final static String HIVE_SQL = "hiveSql";
    public final static String JDBC = "jdbc";
    // 临时表所在的数据库名称
    public final static String TEMP_DB = "tempDb";
    // 临时标存放的HDFS目录
    public final static String TEMP_DB_HDFS_LOCATION = "tempDbPath";

    // 存储文件 hdfs默认的分隔符
    public final static String FIELDDELIMITER="fieldDelimiter";
    public static final String NULL_FORMAT = "nullFormat";
    public static final String HADOOP_CONFIG = "hadoopConfig";
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";

}
