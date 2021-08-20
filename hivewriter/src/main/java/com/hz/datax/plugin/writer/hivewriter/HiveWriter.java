package com.hz.datax.plugin.writer.hivewriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

/**
 * @Author liansongye
 * @create 2021/8/18 9:09 上午
 */
public class HiveWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.HIVE;
    private static final Logger LOG = LoggerFactory.getLogger(Job.class);

    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;
        private CommonRdbWriter.Job commonRdbWriterJob;

        @Override
        public void preCheck(){
            LOG.info("I'm job preCheck");
            this.init();
            this.commonRdbWriterJob.writerPreCheck(this.originalConfig, DATABASE_TYPE);
        }

        @Override
        public void init() {
            LOG.info("I'm job init");
            this.originalConfig = super.getPluginJobConf();
            this.commonRdbWriterJob = new CommonRdbWriter.Job(DATABASE_TYPE);
            this.commonRdbWriterJob.init(this.originalConfig);
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare() {
            LOG.info("I'm job prepare");
            //实跑先不支持 权限 检验
            //this.commonRdbmsWriterJob.privilegeValid(this.originalConfig, DATABASE_TYPE);
            this.commonRdbWriterJob.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            LOG.info("I'm job split");
            return this.commonRdbWriterJob.split(this.originalConfig, mandatoryNumber);
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        @Override
        public void post() {
            LOG.info("I'm job post");
            this.commonRdbWriterJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            LOG.info("I'm job destroy");
            this.commonRdbWriterJob.destroy(this.originalConfig);
        }

    }

    public static class Task extends Writer.Task {
        private Configuration writerSliceConfig;
        private CommonRdbWriter.Task commonRdbWriterTask;

        @Override
        public void init() {
            LOG.info("I'm task init");
            this.writerSliceConfig = super.getPluginJobConf();
            this.commonRdbWriterTask = new CommonRdbWriter.Task(DATABASE_TYPE);
            this.commonRdbWriterTask.init(this.writerSliceConfig);
        }

        @Override
        public void prepare() {
            LOG.info("I'm task preCheck");
            this.commonRdbWriterTask.prepare(this.writerSliceConfig);
        }

        //TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            LOG.info("I'm task startWrite");
            this.commonRdbWriterTask.startWrite(recordReceiver, this.writerSliceConfig,
                    super.getTaskPluginCollector());
        }

        @Override
        public void post() {
            LOG.info("I'm task post");
            this.commonRdbWriterTask.post(this.writerSliceConfig);
        }

        @Override
        public void destroy() {
            LOG.info("I'm task destroy");
            this.commonRdbWriterTask.destroy(this.writerSliceConfig);
        }

        @Override
        public boolean supportFailOver(){
            LOG.info("I'm task supportFailOver");
//            String writeMode = writerSliceConfig.getString(Key.WRITE_MODE);
//            return "replace".equalsIgnoreCase(writeMode);
            return false;
        }

        public static void main(String[] args) throws SQLException {
            Column column = new StringColumn(null);
            System.out.println(column.asString());
//            final Connection con = DBUtil.getConnection(DataBaseType.HIVE, "jdbc:hive2://172.16.255.157:10000", "root", "12345678", "1000");
//
//        try {
//            con.setAutoCommit(false);
//            PreparedStatement preparedStatement = con.prepareStatement("select discharge_order_flag from cd_doctoradvice where id =1");
//            preparedStatement.setString(1, null);
//
//            System.out.println();
//        }catch (Exception e) {
//            e.printStackTrace();
//        }finally {
//            DBUtil.closeDBResources(null, con);
//        }
//            System.out.println();
//
        }

    }
}
