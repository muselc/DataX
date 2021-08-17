package com.hz.datax.plugin.reader.hivereader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;


import org.apache.commons.lang.StringEscapeUtils;

/**
 * Job 中的方法仅执行一次，Task 中方法会由框架启动多个 Task 线程并行执行。
 * <p/>
 * 整个 Reader 执行流程是：
 * <pre>
 * Job类init-->prepare-->split
 *
 * Task类init-->prepare-->startRead-->post-->destroy
 * Task类init-->prepare-->startRead-->post-->destroy
 *
 * Job类post-->destroy
 * </pre>
 */
public class HiveReader {

    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration readerOriginConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;


        @Override
        public void init() {
            LOG.info("init() begin...");
            //获取配置文件信息{parameter 里面的参数}
            this.readerOriginConfig = super.getPluginJobConf();
            this.validate();
            LOG.info("init() ok and end...");

            LOG.info("HiveReader流程说明[1:Reader的HiveQL导入临时表(TextFile无压缩的HDFS) ;2:临时表的HDFS到目标Writer;3:删除临时表]");
        }


        private void validate() {
            this.readerOriginConfig.getNecessaryValue(Key.DEFAULT_FS, HiveReaderErrorCode.DEFAULT_FS_NOT_FIND_ERROR);
            List<String> sqls = this.readerOriginConfig.getList(Key.HIVE_SQL, String.class);
            if (null == sqls || sqls.size() == 0) {
                throw DataXException.asDataXException(HiveReaderErrorCode.SQL_NOT_FIND_ERROR, "您未配置hive sql");
            }
            //check Kerberos
            Boolean haveKerberos = this.readerOriginConfig.getBool(Key.HAVE_KERBEROS, false);
            if (haveKerberos) {
                this.readerOriginConfig.getNecessaryValue(Key.KERBEROS_KEYTAB_FILE_PATH, HiveReaderErrorCode.REQUIRED_VALUE);
                this.readerOriginConfig.getNecessaryValue(Key.KERBEROS_PRINCIPAL, HiveReaderErrorCode.REQUIRED_VALUE);
            }
        }


        @Override
        public List<Configuration> split(int adviceNumber) {
            //按照Hive  sql的个数 获取配置文件的个数
            LOG.info("split() begin...{}" + adviceNumber);
            List<String> sqls = this.readerOriginConfig.getList(Key.HIVE_SQL, String.class);
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();
            Configuration splitedConfig = null;
            for (String querySql : sqls) {
                splitedConfig = this.readerOriginConfig.clone();
                splitedConfig.set(Key.HIVE_SQL, querySql);
                readerSplitConfigs.add(splitedConfig);
            }
            return readerSplitConfigs;
        }

        //全局post
        @Override
        public void post() {
           //do nothing
        }

        @Override
        public void destroy() {
            //do nothing
        }
    }


    public static class Task extends Reader.Task {

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration taskConfig;
        private String hiveSql;
        private String tmpPath;
        private String tableName;
        private String tempHdfsLocation;
        private String fieldDelimiter;
        private String nullFormat;
        private String hive_fieldDelimiter;
        private DFSUtil dfsUtil = null;
        private HashSet<String> sourceFiles;
        private Connection con = null;
        private String createSql;
        private String delSql;

        @Override
        public void init() {
            this.tableName = hiveTableName();
            this.taskConfig = super.getPluginJobConf();//获取job 分割后的每一个任务单独的配置文件
            this.hiveSql = taskConfig.getString(Key.HIVE_SQL);//获取hive sql
            this.tempHdfsLocation = taskConfig.getString(Key.TEMP_DB_HDFS_LOCATION, Constant.TEMP_DATABSE_HDFS_LOCATION_DEFAULT);
            this.fieldDelimiter = taskConfig.getString(Key.FIELDDELIMITER, Constant.FIELDDELIMITER_DEFAULT);
            this.hive_fieldDelimiter = this.fieldDelimiter;
            this.fieldDelimiter = StringEscapeUtils.unescapeJava(this.fieldDelimiter);
            this.taskConfig.set(Key.FIELDDELIMITER, this.fieldDelimiter);//设置hive 存储文件 hdfs默认的分隔符,传输时候会分隔
            this.nullFormat = taskConfig.getString(Key.NULL_FORMAT, Constant.NULL_FORMAT_DEFAULT);
            this.taskConfig.set(Key.NULL_FORMAT, this.nullFormat);
            //判断set语句的结尾是否是分号，不是给加一个
            if (!this.tempHdfsLocation.trim().endsWith("/")) {
                this.tempHdfsLocation = this.tempHdfsLocation + "/";
            }
            this.tmpPath = this.tempHdfsLocation + this.tableName;//创建临时Hive表 存储地址
            LOG.info("配置分隔符后:" + this.taskConfig.toJSON());
            this.dfsUtil = new DFSUtil(this.taskConfig);//初始化工具类
//            con = DBUtil.getConnection(DataBaseType.HIVE, taskConfig.getString(Key.JDBC), "", "");
            con = HiveUtil.getConnect(taskConfig.getString(Key.JDBC), taskConfig.getString(Key.TEMP_DB, Constant.TEMP_DATABASE_DEFAULT));

            createSql = "create temporary table "
                    + this.tableName + " ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + this.hive_fieldDelimiter
                    + "' STORED AS TEXTFILE "
                    + " as " + this.hiveSql;
            delSql = " drop table " + this.tableName;
        }

        @Override
        public void prepare() {
            LOG.info("创建临时表 ----> : {}", createSql);
            boolean execute = false;
            try (PreparedStatement preparedStatement = con.prepareStatement(createSql)) {
                 execute = preparedStatement.execute();
            } catch (Exception e) {
                e.printStackTrace();
                closeCon();
            }
            LOG.info("创建hive 临时表结果 ----> {}" , execute);

            LOG.info("prepare(), start to getAllFiles...");
            List<String> path = new ArrayList<String>();
            path.add(tmpPath);
            this.sourceFiles = dfsUtil.getAllFiles(path, Constant.TEXT);
            LOG.info(String.format("您即将读取的文件数为: [%s], 列表为: [%s]", this.sourceFiles.size(), StringUtils.join(this.sourceFiles, ",")));
        }

        @Override
        public void startRead(RecordSender recordSender) {

            LOG.info("read start");

            //读取临时hive表的hdfs文件
            for (String sourceFile : this.sourceFiles) {
                LOG.info(String.format("reading file : [%s]", sourceFile));

                InputStream inputStream = dfsUtil.getInputStream(sourceFile);
                UnstructuredStorageReaderUtil.readFromStream(inputStream, sourceFile, this.taskConfig, recordSender, this.getTaskPluginCollector());
                if (recordSender != null) {
                    recordSender.flush();
                }
            }
            LOG.info("end read source files...");
        }


        //只是局部post  属于每个task
        @Override
        public void post() {
            LOG.info("one task hive read post...");
            deleteTmpTable();
        }


        @Override
        public void destroy() {
            closeCon();
            LOG.info("hive read destroy...");
        }

        //关闭con
        private void closeCon() {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }


        //创建hive临时表名称
        private String hiveTableName() {

            StringBuilder str = new StringBuilder();
            FastDateFormat fdf = FastDateFormat.getInstance("yyyyMMdd");

            str.append(Constant.TEMP_TABLE_NAME_PREFIX).append(fdf.format(new Date()))
                    .append("_").append(UUID.randomUUID().toString().replace("-", ""));

            return str.toString().toLowerCase();
        }

        //删除临时表
        private void deleteTmpTable() {
            LOG.info("清空数据:hiveSql ----> :" + delSql);
            boolean execute = false;
            try (final Statement statement = con.createStatement()) {
                execute = statement.execute(delSql);
            } catch (Exception e) {
                e.printStackTrace();
                closeCon();
            }
            LOG.info("清空数据结果 ----> {}" , execute);
        }

    }

}
