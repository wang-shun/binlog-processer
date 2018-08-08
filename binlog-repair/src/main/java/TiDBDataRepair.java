public class TiDBDataRepair implements DataRepair {

    @Override
    public void dataRepairById(String fileName, String dataBase, String tableName, String id) {
        String tableExistSql = "";
        String getSchemaSql = "";
        String upsertSql = "";
        // TODO: 2018/8/7
        //1.根据参数信息读取avro文件
        //2.根据id从文件中找到这行数据
        //3.将这条数据插入更新到TiDB(首先要判断目标数据库中该表是否存在)
    }

    @Override
    public void dataRepairByTime(String fileName, String instance, String dataBase, String tableName, String startTime, String endTime) {
        // TODO: 2018/8/7
        //1.查询出某个实例某天的avro文件，可以指定database、table
        //2.依次读取这些avro文件的数据进行重做
    }

    @Override
    public void dataRepairByFile(String fileName) {
        //读取某个文件，并将所有记录解析出来重新插入到数据库
    }

    @Override
    public void dataRepairSchedule() {

    }

    @Override
    public void dataRepairByRecordNum(int recordNum) {

    }
}
