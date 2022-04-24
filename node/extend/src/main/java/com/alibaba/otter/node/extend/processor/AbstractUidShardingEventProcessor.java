package com.alibaba.otter.node.extend.processor;

import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;

/**
 * 一对多分片同步时判断数据是否属于新的分片
 * @author wangyi
 */
public abstract class AbstractUidShardingEventProcessor extends RemoveIdEventProcessor {

    @Override
    public boolean process(EventData eventData) {
        super.process(eventData);
        for (EventColumn column : eventData.getColumns()) {
            if("uid".equalsIgnoreCase(column.getColumnName())){
                Long uid = Long.valueOf(column.getColumnValue());
                return uid % getCurrentDbIndex() == getCurrentTableIndex();
            }
        }
        return false;
    }

    /**
     * 获取当前库索引
     * @return
     */
    public abstract int getCurrentDbIndex();

    /**
     * 获取当前表索引
     * @return
     */
    public abstract int getCurrentTableIndex();
}
