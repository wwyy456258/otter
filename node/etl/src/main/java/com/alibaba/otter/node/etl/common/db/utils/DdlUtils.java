package com.alibaba.otter.node.etl.common.db.utils;

import java.util.Map;

import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;

/**
 * 解析一下DDL的完整语法
 * 
 * @author agapple 2017年4月6日 下午1:07:53
 * @since 4.2.14
 */
public class DdlUtils {

    public static String convert(String sql, String sourceSchema, String sourceTable, String targetSchema,
                                 String targetTable) {
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();

        StringBuilder out = new StringBuilder();
        OtterMyqlOutputVisitor visitor = new OtterMyqlOutputVisitor(out,
            sourceSchema,
            sourceTable,
            targetSchema,
            targetTable);
        stmt.accept(visitor);
        return out.toString();
    }

    public static class OtterMyqlOutputVisitor extends MySqlOutputVisitor {

        private String              targetSchema;
        private String              targetTable;
        private String              sourceSchema;
        private String              sourceTable;

        public OtterMyqlOutputVisitor(Appendable appender, String sourceSchema, String sourceTable,
                                      String targetSchema, String targetTable){
            super(appender);
            this.sourceSchema = sourceSchema;
            this.sourceTable = sourceTable;
            this.targetSchema = targetSchema;
            this.targetTable = targetTable;
        }

        private void processTableName(SQLExpr sqlName) {
            if (sqlName instanceof SQLPropertyExpr) {
                SQLIdentifierExpr owner = (SQLIdentifierExpr) ((SQLPropertyExpr) sqlName).getOwner();
                String oldSchem = unescapeName(owner.getName());
                String oldTable = unescapeName(((SQLPropertyExpr) sqlName).getName());
                if ((sourceSchema == null || oldSchem.equalsIgnoreCase(sourceSchema))
                    && (sourceTable == null || oldTable.equalsIgnoreCase(sourceTable))) { // rename需要匹配表名
                    owner.setName("`" + targetSchema + "`");
                    ((SQLPropertyExpr) sqlName).setName("`" + targetTable + "`");
                }
            } else if (sqlName instanceof SQLIdentifierExpr) {
                String oldTable = unescapeName(((SQLIdentifierExpr) sqlName).getName());
                if (sourceTable == null || oldTable.equalsIgnoreCase(sourceTable)) {
                    // try {
                    // // 拼上一个schema
                    // this.appender.append("`" + targetSchema + "`");
                    // } catch (IOException e) {
                    // throw new RuntimeException(e);
                    // }
                    ((SQLIdentifierExpr) sqlName).setName("`" + targetTable + "`");
                }
            } else {
                throw new RuntimeException("not support SQLName:" + sqlName);
            }

            sqlName.accept(this);
        }

        private String unescapeName(String name) {
            if (name == null || name.length() <= 0) {
                return name;
            }
            if (name.charAt(0) != '`') {
                return name;
            }
            if (name.charAt(name.length() - 1) != '`') {
                throw new IllegalArgumentException("id start with a '`' must end with a '`', id: " + name);
            }
            StringBuilder sb = new StringBuilder(name.length() - 2);
            final int endIndex = name.length() - 1;
            boolean hold = false;
            for (int i = 1; i < endIndex; ++i) {
                char c = name.charAt(i);
                if (c == '`' && !hold) {
                    hold = true;
                    continue;
                }
                hold = false;
                if (c >= 'a' && c <= 'z') {
                    c -= 32;
                }
                sb.append(c);
            }
            return sb.toString();
        }

        public boolean visit(MySqlCreateTableStatement x) {

            return false;
        }

        public boolean visit(SQLAlterTableStatement x) {
            return false;
        }

        @Override
        public boolean visit(MySqlRenameTableStatement.Item x) {
            processTableName(x.getName());
            print0(ucase ? " TO " : " to ");
            processTableName(x.getTo());
            return false;
        }

        public boolean visit(SQLExprTableSource x) {
            processTableName(x.getExpr());
            if (x.getAlias() != null) {
                print(' ');
                print0(x.getAlias());
            }

            for (int i = 0; i < x.getHintsSize(); ++i) {
                print(' ');
                x.getHints().get(i).accept(this);
            }

            if (x.getPartitionSize() > 0) {
                print0(ucase ? " PARTITION (" : " partition (");
                printlnAndAccept(x.getPartitions(), ", ");
                print(')');
            }

            return false;
        }
    }

}
