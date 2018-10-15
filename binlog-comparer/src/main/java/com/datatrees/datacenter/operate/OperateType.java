package com.datatrees.datacenter.operate;

public enum OperateType {
    /**
     * 创建
     */
    Create(0),
    /**
     * 更新
     */
    Update(1),
    /**
     * 删除
     */
    Delete(2),
    /**
     * 多个时间按时间先后覆盖
     */
    Unique(3);

    private int value;

    OperateType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
