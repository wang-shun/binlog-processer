package com.datatrees.datacenter.main;

import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.repair.hive.HiveDataRepair;
import com.datatrees.datacenter.table.CheckResult;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author personalc
 * repair for specail database tables
 */
public class HiveRepairForTable {

    private static HiveDataRepair dataRepair = new HiveDataRepair();

    public static void main(String[] args) {
        String dataBaseTable = "point.t_point_channel,loandb.t_cust_info,loandb.t_cust_channel,loandb.t_channel,loandb.t_channel_app,loandb.t_channel_company,loandb.t_channel_valuation,loandb.t_treefinance_app,loandb.t_user_data_supply_status,loandb.t_user_pre_credit_risk,loandb.t_fraud_rule_log,loandb.t_user_credit_risk_history,loandb.t_cust_ext_info,loandb.t_order_original,credit_audit.t_audit_job,loandb.t_loan_order,loandb.t_predict_info,loandb.t_bank_account,loandb.t_repayment_schedule,loandb.t_device_channel,loandb.t_user_score_card";
        String[] dataBaseTables = dataBaseTable.split(",");
        List<String> tables = Arrays.asList(dataBaseTables);
        tables.parallelStream().forEachOrdered(x -> {
            try {
                String[] databaseTables = x.split("\\.");
                String checkDataBase = databaseTables[0];
                String checkTable = databaseTables[1];
                List<Map<String, Object>> dataMapList = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), "binlog", "select db_instance,database_name,table_name,file_partitions operate_type,file_name from t_binlog_check_hive where repair_status=0 and type='create' and database_name='" + checkDataBase + "' and table_name='" + checkTable + "'");
                if (dataMapList != null && dataMapList.size() > 0) {
                    for (Map<String, Object> map : dataMapList) {
                        CheckResult checkResult = new CheckResult();
                        String dbInstance = (String) map.get("db_instance");
                        dbInstance = dbInstance.replaceAll("_", ".");
                        String dataBase = String.valueOf(map.get("database_name"));
                        String partition = String.valueOf(map.get("file_partitions"));
                        String partitionType = "create";
                        String tableName = String.valueOf(map.get("table_name"));
                        String fileName = String.valueOf(map.get("file_name"));

                        checkResult.setDbInstance(dbInstance);
                        checkResult.setDataBase(dataBase);
                        checkResult.setFilePartition(partition);
                        checkResult.setPartitionType(partitionType);
                        checkResult.setTableName(tableName);
                        checkResult.setFileName(fileName);

                        dataRepair.repairByIdList(checkResult, "t_binlog_check_hive");
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }
}
