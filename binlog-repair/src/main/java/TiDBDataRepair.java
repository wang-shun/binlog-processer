public class TiDBDataRepair implements DataRepair {

    @Override
    public void dataRepairById(String fileName, String dataBase, String tableName, String id) {
        String tableExistSql = "";
        String getSchemaSql = "";
        String upsertSql = "";
    }

    @Override
    public void dataRepairByTime(String fileName, String dataBase, String tableName, String startTime, String endTime) {

    }

    @Override
    public void dataRepairByFile(String fileName) {

    }
}
