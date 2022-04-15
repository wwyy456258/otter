package com.alibaba.otter.node.etl.extend.processor;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.node.etl.select.selector.MessageParser;
import com.alibaba.otter.shared.common.model.config.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认的基于数据库主键的逻辑
 *
 * @author wangyi
 */
public class OrdersIsKeyProcessor implements IsKeyProcessor {
    private static final Logger logger = LoggerFactory.getLogger(OrdersIsKeyProcessor.class);

    @Override
    public boolean isKey(MessageParser.TableInfoHolder tableHolder, String tableName, CanalEntry.Column column, Pipeline pipeline, CanalEntry.RowData rowData) {
        return ("order_id").equals(column.getName());
    }
}
