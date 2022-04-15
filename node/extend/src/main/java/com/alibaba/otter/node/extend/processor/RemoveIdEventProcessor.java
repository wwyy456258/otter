package com.alibaba.otter.node.extend.processor;

import com.alibaba.otter.shared.etl.extend.processor.EventProcessor;
import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;

/**
 * 清除数据中id的值
 * @author wangyi
 */
public class RemoveIdEventProcessor implements EventProcessor {

    @Override
    public boolean process(EventData eventData) {
        removeId(eventData);
        return true;
    }

    private void removeId(EventData eventData){
        for (EventColumn column : eventData.getKeys()) {
            if("id".equalsIgnoreCase(column.getColumnName())){
                column.setColumnValue(null);
            }
        }
        for (EventColumn column : eventData.getColumns()) {
            if("id".equalsIgnoreCase(column.getColumnName())){
                column.setColumnValue(null);
            }
        }
    }
}
