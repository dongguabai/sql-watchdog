package io.github.dongguabai;

import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.SystemMetaObject;
import org.apache.ibatis.session.ResultHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Statement;
import java.util.List;
import java.util.Properties;

/**
 * @author dongguabai
 * @date 2024-02-08 10:01
 */
@Intercepts({
        @Signature(type = StatementHandler.class, method = "query", args = {Statement.class, ResultHandler.class})
})
public class ResultSizeMonitorInterceptor implements Interceptor {

    private static final Logger log = LoggerFactory.getLogger(ResultSizeMonitorInterceptor.class);

    private static final String H = "h";

    private static final String TARGET = "target";

    private static final String NEXTVAL = "NEXTVAL";

    private static final String PROJECT = "maf";

    private boolean enabled;

    private String emails;

    private String excludeSqlIds;

    private int threshold;

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        Object result = invocation.proceed();
        if (!enabled) {
            return result;
        }
        SqlInfo sqlInfo = new SqlInfo();
        try {
            if (result instanceof List) {
                List list = (List) result;
                if (list.size() > threshold) {
                    StatementHandler statementHandler = (StatementHandler) invocation.getTarget();
                    MetaObject metaStatementHandler = SystemMetaObject.forObject(statementHandler);
                    while (metaStatementHandler.hasGetter(H)) {
                        Object object = metaStatementHandler.getValue(H);
                        metaStatementHandler = SystemMetaObject.forObject(object);
                    }
                    while (metaStatementHandler.hasGetter(TARGET)) {
                        Object object = metaStatementHandler.getValue(TARGET);
                        metaStatementHandler = SystemMetaObject.forObject(object);
                    }
                    MappedStatement mappedStatement = (MappedStatement) metaStatementHandler.getValue("delegate.mappedStatement");
                    BoundSql boundSql = statementHandler.getBoundSql();
                    sqlInfo.sql = boundSql.getSql();
                    sqlInfo.id = mappedStatement.getId();
                    sqlInfo.params = boundSql.getParameterObject().toString();
                    trigger(sqlInfo, list);
                }
            }
        } catch (Throwable e) {
            log.error("ResultSizeMonitorInterceptor.intercept error.", e);
        }
        return result;
    }

    private void trigger(SqlInfo sqlInfo, List list) {
        // Can perform monitoring or send alert messages, etc.
    }


    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties properties) {
        this.enabled = Boolean.parseBoolean(properties.getProperty("enabled", "false"));
        this.emails = properties.getProperty("emails", "");
        this.threshold = Integer.parseInt(properties.getProperty("threshold", "5000"));
        this.excludeSqlIds = properties.getProperty("excludeSqlIds", "");

        log.info("ResultSizeMonitorInterceptor is enabled: {}", enabled);
        log.info("Threshold: {}", threshold);
        log.info("Emails: {}", emails);
        log.info("trigger exclude SQL IDs: {}", excludeSqlIds);
    }

    class SqlInfo {
        String id = "";
        String sql = "";
        String params = "";

        public SqlInfo() {
        }

        @Override
        public String toString() {
            return "SqlInfo{" +
                    "id='" + id + '\'' +
                    ", sql='" + sql + '\'' +
                    ", params=" + params +
                    '}';
        }
    }

}
