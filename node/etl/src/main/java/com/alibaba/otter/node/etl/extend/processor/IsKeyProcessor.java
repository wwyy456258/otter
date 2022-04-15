package com.alibaba.otter.node.etl.extend.processor;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.node.etl.select.selector.MessageParser;
import com.alibaba.otter.shared.common.model.config.pipeline.Pipeline;

/**
 * 主键判断扩展
 * 用来扩展除数据库主键外的业务主键
 * @author wangyi
 */
public interface IsKeyProcessor {

    /**
     * 是否主键
     * @param tableHolder
     * @param tableName
     * @param column
     * @param pipeline
     * @param rowData
     * @return
     */
    boolean isKey(MessageParser.TableInfoHolder tableHolder, String tableName, CanalEntry.Column column, Pipeline pipeline, CanalEntry.RowData rowData);
}
