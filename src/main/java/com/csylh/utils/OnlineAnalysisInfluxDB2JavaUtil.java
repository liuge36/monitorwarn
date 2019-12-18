package com.csylh.utils;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

/**
 * Description:
 *
 * @Author: 留歌36
 * @Date: 2019/12/17 11:11
 */
public class OnlineAnalysisInfluxDB2JavaUtil {

    private static InfluxDB influxDB;

    private static String defaultRetentionPolicy(String version) {
        if (version.startsWith("0.") ) {
            return "default";
        } else {
            return "autogen";
        }
    }

    public static void insertInfluDB(String value){
        // 定义连接influxdb
        influxDB = InfluxDBFactory
                .connect("http://120.27.243.91:8086", "admin", "admin");
        // 获取版本协议
        String retentionPolicy = defaultRetentionPolicy(influxDB.version());
        // 这里只有一个节点，就选择ConsistencyLevel.ONE
        influxDB.write("online_log_analysis", retentionPolicy, InfluxDB.ConsistencyLevel.ONE, value);

    }

    public static void main(String[] args) {
        OnlineAnalysisInfluxDB2JavaUtil.insertInfluDB("logtype_count,logtype=FATAL count=333");
    }
}
