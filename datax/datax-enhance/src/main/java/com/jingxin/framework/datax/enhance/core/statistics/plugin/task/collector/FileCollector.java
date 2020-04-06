package com.jingxin.framework.datax.enhance.core.statistics.plugin.task.collector;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.plugin.task.StdoutPluginCollector;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;


public class FileCollector extends StdoutPluginCollector {

    private static final Logger LOG = LoggerFactory.getLogger("DirtyRecordLog");

    private Long jobId = 0L;

    public FileCollector(Configuration configuration, Communication communication, PluginType type) {
        super(configuration, communication, type);
        jobId = configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        MDC.put("jobId", jobId.toString());
    }

    @Override
    public void collectDirtyRecord(Record dirtyRecord, Throwable t, String errorMessage) {
        if (maxLogNum.intValue() < 0 || currentLogNum.intValue() < maxLogNum.intValue()) {
            LOG.error(this.formatDirty(dirtyRecord, t, errorMessage));
        }
    }
}
