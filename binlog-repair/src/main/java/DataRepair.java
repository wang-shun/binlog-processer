public interface DataRepair {
    /**
     * 根据时间段进行批量修复
     *
     * @param startTime
     * @param endTime
     * @param partitionType
     */
    void repairByTime(String startTime, String endTime, String partitionType);

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
