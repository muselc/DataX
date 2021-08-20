package com.hz.datax.plugin.reader.hivereader;


import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * @Author liansongye
 * @create 2021/8/17 10:28 上午
 */

public class HiveReader extends Reader {

    private static final Logger LOG = LoggerFactory.getLogger(Job.class);
    private static final DataBaseType DATABASE_TYPE = DataBaseType.HIVE;

    public static class Job extends Reader.Job {


        private Configuration originalConfig = null;
        private CommonRdbReader.Job commonRdbmsReaderJob;

        @Override
        public void init() {
            LOG.info("I'm job init");

            this.originalConfig = super.getPluginJobConf();
            this.originalConfig.set(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE, Integer.MIN_VALUE);
            this.commonRdbmsReaderJob = new CommonRdbReader.Job(DATABASE_TYPE);
            this.commonRdbmsReaderJob.init(this.originalConfig);
        }

        @Override
        public void preCheck(){
            LOG.info("I'm job preCheck");
            init();
            this.commonRdbmsReaderJob.preCheck(this.originalConfig,DATABASE_TYPE);
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.info("I'm job split");
            return this.commonRdbmsReaderJob.split(this.originalConfig, adviceNumber);
        }

        @Override
        public void post() {
            LOG.info("I'm job post");
            this.commonRdbmsReaderJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            LOG.info("I'm job destroy");
            this.commonRdbmsReaderJob.destroy(this.originalConfig);
        }

    }

    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;
        private CommonRdbReader.Task commonRdbmsReaderTask;

        @Override
        public void init() {
            LOG.info("I'm task init");
            this.readerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbReader.Task(DATABASE_TYPE,super.getTaskGroupId(), super.getTaskId());
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);

        }

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.info("I'm task startRead");
//            int fetchSize = this.readerSliceConfig.getInt(Constant.FETCH_SIZE);

            int fetchSize=Integer.MAX_VALUE;

            this.commonRdbmsReaderTask.startRead(this.readerSliceConfig, recordSender,
                    super.getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post() {
            LOG.info("I'm task post");
            this.commonRdbmsReaderTask.post(this.readerSliceConfig);
        }

        @Override
        public void destroy() {
            LOG.info("I'm task destroy");
            this.commonRdbmsReaderTask.destroy(this.readerSliceConfig);
        }

    }

    public static void main(String[] args) {

        //        String querySql = "select docadvice_no, org_code, medi_record_no, pati_medi_id, inpatient_serial_no, inpatient_times, docadvise_group_num, start_time, end_time, open_date, open_dept_code, open_dept_name, open_doct_eeid, open_doct_name, last_execute_date, item_type_code, item_type_name, docadvise_item_code, docadvise_item_name, docadvise_cate_code, docadvise_cate_name, docadvise_current_state, drug_code, drug_produce_code, drug_name, drug_spec, way_drug_code, way_drug_name, drug_use_dose, drug_use_dose_unit, drug_use_amount, drug_use_amount_unit, drug_use_frequency_num, drug_use_frequency_name, drug_dosage_from_code, drug_dosage_from_name, drug_unit, drug_price, drug_abbr, drug_quantity, packet_amount, decoction_flag, main_sec_drug_flag, discharge_order_flag, emergency_flag, docadvise_state, docadvise_recipel_type, inpat_ward_num, inpat_ward_name, cancel_durg_flag, cancel_flag, collect_time, collect_serial_no, doc_advise, remark, skin_test_flag, doc_flag, antibiotics_flag, infusion_flag, pat_condition, doctor_group, doctor_group_name, stop_date, stop_doc, stop_doc_name from (select *,row_number() over (partition by collect_serial_no order by collect_time desc)  num from v_docadvice) as t where t.num = 1";
//        final Connection con = DBUtil.getConnection(DataBaseType.HIVE, "jdbc:hive2://172.16.255.157:10000", "root", "root");
//        ResultSet rs = null;
//        try {
//            con.setAutoCommit(false);
//            Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
//                    ResultSet.CONCUR_READ_ONLY);
//            rs = DBUtil.query(stmt, querySql);
//            final ResultSetMetaData metaData = rs.getMetaData();
//            while (rs.next()) {
//                final String string = rs.getString(1);
//                System.out.println();
//            }
//            System.out.println();
//        }catch (Exception e) {
//            e.printStackTrace();
//        }finally {
//            DBUtil.closeDBResources(null, con);
//        }

    }

}
