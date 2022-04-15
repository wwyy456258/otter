package com.alibaba.otter.node.etl.extend.processor;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.node.etl.select.exceptions.SelectException;
import com.alibaba.otter.node.etl.select.selector.MessageParser;
import com.alibaba.otter.shared.common.model.config.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认的基于数据库主键的逻辑
 *
 * @author wangyi
 */
public class DefaultIsKeyProcessor implements IsKeyProcessor {
    private static final Logger logger = LoggerFactory.getLogger(DefaultIsKeyProcessor.class);

    @Override
    public boolean isKey(MessageParser.TableInfoHolder tableHolder, String tableName, CanalEntry.Column column, Pipeline pipeline, CanalEntry.RowData rowData) {
        boolean isEKey = column.getIsKey();
        if (tableHolder == null || tableHolder.getTable() == null || !tableHolder.isUseTableTransform()) {
            return isEKey;
        }

        org.apache.ddlutils.model.Column dbColumn = tableHolder.getTable().findColumn(column.getName(), false);
        if (dbColumn == null) {
            // 可能存在ddl，重新reload一下table
            tableHolder.reload();
            dbColumn = tableHolder.getTable().findColumn(column.getName(), false);
            if (dbColumn == null) {
                throw new SelectException(String.format("not found column[%s] in table[%s]",
                        column.getName(),
                        tableHolder.getTable().toVerboseString()));
            }
        }

        boolean isMKey = dbColumn.isPrimaryKey();
        if (isMKey != isEKey) {
            logger.info("table [{}] column [{}] is not match , isMeky: {}, isEkey {}",
                    new Object[] { tableName, column.getName(), isMKey, isEKey });
        }
        return isMKey;
    }
}
