public interface DataRepair {
    /**
     * 根据分区进行修复
     * @param dbInstance
     * @param dataBase
     * @param tableName
     * @param partition
     * @param type
     */
    void repairByTime(String dbInstance, String dataBase, String tableName, String partition, String type) ;

    /**
     * 根据文件名进行批量修复
     *
     * @param fileName
     * @param partitionType
     */
    void repairByFile(String fileName, String partitionType);

    /**
     * 根据错误记录数进行修复
     *
     * @param recordNum
     * @param partitionType
     */
    void repairByRecordNum(int recordNum, String partitionType);

    /**
     * 定时修复(定时执行上述一种)
     *
     * @param partitionType
     */
    void repairSchedule(String partitionType);
}
