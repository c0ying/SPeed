package com.alibaba.datax.core.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.ExecuteMode;

public class CoreConfigurationUtil {

    public static ExecuteMode getExecuteMode(Configuration core){
        String executeMode = core.getString(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE);
        return ExecuteMode.toExecuteMode(executeMode);
    }
}
