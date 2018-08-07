public interface DataRepair {
    public void dataRepairById(String fileName,String dataBase,String tableName,String id);

    public void dataRepairByTime(String fileName,String dataBase,String tableName,String startTime,String endTime);

    public void dataRepairByFile(String fileName);
}
