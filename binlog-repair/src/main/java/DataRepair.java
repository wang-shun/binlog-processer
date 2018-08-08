public interface DataRepair {
    /**
     * 根据ID等信息进行单条数据修复
     *
     * @param fileName
     * @param dataBase
     * @param tableName
     * @param id
     */
    void dataRepairById(String fileName, String dataBase, String tableName, String id);

    /**
     * 根据时间段进行批量修复
     *
     * @param fileName
     * @param instance
     * @param dataBase
     * @param tableName
     * @param startTime
     * @param endTime
     */
    void dataRepairByTime(String fileName, String instance, String dataBase, String tableName, String startTime, String endTime);

    /**
     * 根据文件名进行批量修复
     *
     * @param fileName
     */
    void dataRepairByFile(String fileName);

    /**
     * 定时修复
     */
    void dataRepairSchedule();

    /**
     * 根据错误记录数进行修复
     *
     * @param recordNum
     */
    void dataRepairByRecordNum(int recordNum);
}
