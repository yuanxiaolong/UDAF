package com.yxl;

import com.yxl.util.JsonFilterUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 保留指定tagId的标签
 */
public class UDFFilter extends UDF {

    public Text evaluate(String input,int tagId){

        if (StringUtils.isBlank(input)){
            return new Text("");
        }
        return new Text(JsonFilterUtil.filter(input,tagId));
    }

}