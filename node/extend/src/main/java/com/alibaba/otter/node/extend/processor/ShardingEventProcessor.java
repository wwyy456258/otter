package com.alibaba.otter.node.extend.processor;

import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;

/**
 * 一对多分片同步时判断数据是否属于新的分片
 */
public class ShardingEventProcessor extends RemoveIdEventProcessor {

    public static final int DB_NUM = 2;

    public static final int CURRENT_DB_INDEX = 0;

    @Override
    public boolean process(EventData eventData) {
        super.process(eventData);
        for (EventColumn column : eventData.getColumns()) {
            if("uid".equalsIgnoreCase(column.getColumnName())){
                Long uid = Long.valueOf(column.getColumnValue());
                return uid % DB_NUM == CURRENT_DB_INDEX;
            }
        }
        return false;
    }
}
