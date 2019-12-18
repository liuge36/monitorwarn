package com.csylh.utils;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Description:
 *   【日志信息】 出现自定义alert词的次数
 *   功能: 主要是为了在spark streaming中 【更新】 一个广播变量
 *
 * @Author: 留歌36
 * @Date: 2019/8/8 23:12
 */
public class OnlineAnalysisBroadcastAlertAppUtil {

    /**
     * 上次time
     */
    private static Date lastUpdatedAt = Calendar.getInstance().getTime();

    private static String url = "jdbc:mysql://120.27.243.91:3306/onlineloganalysis?useSSL=false&useUnicode=true&characterEncoding=UTF-8";
    private static String user = "root";
    private static String password = "P@ssw0rd";
    private static String altertable = "alertinfo_config";


    /**
     * 这里是 饿汉式
     */
    private OnlineAnalysisBroadcastAlertAppUtil(){}

    private static OnlineAnalysisBroadcastAlertAppUtil obj = new OnlineAnalysisBroadcastAlertAppUtil();

    public static OnlineAnalysisBroadcastAlertAppUtil getInstance() {
        return obj;
    }

    public Broadcast<List<String>> updateAndGet(SparkSession sparkSession, Broadcast<List<String>> bcAlertList){
        //当前time
        Date currentDate = Calendar.getInstance().getTime();
        //time差值
        long diff = currentDate.getTime()-lastUpdatedAt.getTime();

        //Lets say we want to refresh every 1 min = 60000 ms
        if (bcAlertList == null || diff >= 10000) {
            if (bcAlertList != null) {
                //删除存储
                bcAlertList.unpersist();
            }
            //再次更新上次time
            lastUpdatedAt = new Date(System.currentTimeMillis());


            // 定义sqlcontext
            SQLContext sqlc= sparkSession.sqlContext();
//            Properties connectionProperties = new Properties();
//            connectionProperties.put("user", user);
//            connectionProperties.put("password", password);
//            Dataset<Row> alterDs = sqlc.read()
//                    .jdbc(url, altertable, connectionProperties);//读取mysql的表数据

            Dataset<Row> alterDs =  sqlc.read().format("jdbc")
                    .option("url", url)
                    .option("dbtable", altertable)
                    .option("user", user)
                    .option("password", password)
                    .load();

            List<String> alertList= new ArrayList<String>();
            //返回一个list对象 返回DS的所有行
            List<Row> warninfo = alterDs.collectAsList();
            //循环add
            for(Row row_warninfo:warninfo){
                //keywords列
                alertList.add(row_warninfo.get(0).toString());
            }
            //定义广播变量bcAlertList
            bcAlertList= JavaSparkContext
                    .fromSparkContext(sparkSession.sparkContext())
                    .broadcast(alertList);
        }

        return bcAlertList;
    }


}
