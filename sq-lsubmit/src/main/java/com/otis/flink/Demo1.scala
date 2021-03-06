package com.otis.flink

import com.otis.flink.udaf.CustomCount
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

object Demo1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tableEnv = StreamTableEnvironment.create(env, settings)

    tableEnv.registerFunction("customcount", new CustomCount)
    val source1 =
      """
        |    CREATE TABLE kafka_apply_info_200 (
        |      SESSION_ID STRING,
        |      APP_NO STRING,
        |      CUST_ID STRING,
        |      BUSINESS_TYPE_CD STRING,
        |      BUSINESS_TYPE_NAME STRING,
        |      CUST_SOURCE STRING,
        |      CHANNEL_SOURCE STRING,
        |      APPLY_TIME BIGINT,
        |      et AS TO_TIMESTAMP(FROM_UNIXTIME(APPLY_TIME/1000)),
        |      WATERMARK FOR et AS et - INTERVAL '5' SECOND
        |    )
        |    WITH (
        |      'connector.type' = 'kafka',
        |      'connector.version' = 'universal',
        |      'connector.topic' = 'kafkaCreditApplyInfo_1234',
        |      'connector.properties.group.id'='dev_flink',
        |      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
        |      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
        |      'format.type' = 'json',
        |      'update-mode' = 'append',
        |      'connector.startup-mode' = 'latest-offset'
        |      )
        |""".stripMargin
    tableEnv.sqlUpdate(source1)

    val source2 =
      """
        |   CREATE TABLE kafka_result_info_200 (
        |      SESSION_ID STRING,
        |      APP_NO  STRING,
        |      CUST_ID STRING,
        |      CREDIT_NO  STRING,
        |      BUSINESS_TYPE_CD STRING,
        |      STATE_CODE STRING,
        |      CREDIT_CODE  STRING,
        |      REFUSE_REASON STRING,
        |      INTEREST_RATE  DOUBLE,
        |      CREDIT_LIMIT DOUBLE,
        |     REPAY_MODE_CD STRING,
        |     LOAN_TERM INTEGER,
        |     CREDIT_SCORE_1  DOUBLE, -- 客户卡评分
        |     CREDIT_SCORE_2 DOUBLE,
        |     CREDIT_SCORE_3  DOUBLE,
        |     ANTI_FRAUD_SCORE_1  DOUBLE,
        |     ANTI_FRAUD_SCORE_2  DOUBLE,
        |     WHITE_LIST int, -- 命中白名单标识 1 命中 0 未命中
        |     APPLY_SCORE int,--产品卡评分
        |     BLACK_LIST int, -- 命中黑名单标识 1 命中 0 未命中
        |     CREDIT_TIME  BIGINT,
        |     et AS TO_TIMESTAMP(FROM_UNIXTIME(CREDIT_TIME/1000)),
        |    WATERMARK FOR et AS et - INTERVAL '5' SECOND
        |    )WITH (
        |      'connector.type' = 'kafka',
        |      'connector.version' = 'universal',
        |      'connector.topic' = 'kafkaCreditResultInfo_1234',
        |      'connector.properties.group.id'='dev_flink',
        |      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
        |      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
        |      'format.type' = 'json',
        |      'update-mode' = 'append',
        |      'connector.startup-mode' = 'latest-offset'
        |    )
        |""".stripMargin
    tableEnv.sqlUpdate(source2)

    val nowview1 =
      """
        |create view now_view1 as
        |select
        |    BUSINESS_TYPE_CD,
        |    count (1) as APP_C_CT,
        |    max (APPLY_TIME) as ts
        |from kafka_apply_info_200 where BUSINESS_TYPE_CD='suixindai' group by BUSINESS_TYPE_CD
        |""".stripMargin
//    tableEnv.sqlUpdate(nowview1)

    val nv2 =
      """
        |create view now_view2 as
        |select
        |    BUSINESS_TYPE_CD,
        |    sum (if(CREDIT_CODE='0',1,0))                       as      ACC_C_CT,       -- 截止当前时点前贷款授信申请通过笔数
        |    sum (if(WHITE_LIST=1,1,0))                          as      APP_C_WL_CCT,   -- 截止当前时点前贷款授信申请且命中白名单客户的笔数
        |    sum (if(CREDIT_CODE='0' and WHITE_LIST=1,1,0))      as      ACC_C_WL_CCT,   -- 截止当前时点前贷款授信申请通过且命中白名单客户的笔数
        |    sum (if(WHITE_LIST=0,1,0))                          as      APP_C_UWL_CCT,  -- 截止当前时点前贷款授信申请且wei命中白名单客户的笔数
        |    sum (if(CREDIT_CODE='0' and WHITE_LIST=0,1,0))      as      ACC_C_UWL_CCT,  -- 截止当前时点前贷款授信申请通过且wei命中白名单客户的笔数
        |    sum (if(BLACK_LIST=1,1,0))                          as      AF_ACC_C_CT,    -- 截止当前时点前贷款授信申请反欺诈通过笔数
        |    avg (CREDIT_SCORE_1)                                as      AVG_ACC_CCSCT,  -- 截止当前时点前贷款授信申请通过客户的客户卡评分均值
        |    avg(APPLY_SCORE)                                    as      AVG_ACC_PCSCT   -- 截止当前时点前贷款授信申请通过客户的产品卡评分均值
        |from kafka_result_info_200 where BUSINESS_TYPE_CD='suixindai' group by BUSINESS_TYPE_CD
        |""".stripMargin
    tableEnv.sqlUpdate(nv2)

    val nv3 =
      """
        |create view now_view3 as
        |select
        |now_view1.BUSINESS_TYPE_CD,
        |ts,
        |APP_C_CT,
        |ACC_C_CT,
        |cast(if(APP_C_CT=0,-999,ACC_C_CT/APP_C_CT) as float)                    as PR_CT,
        |APP_C_WL_CCT,
        |ACC_C_WL_CCT,
        |cast(if(APP_C_WL_CCT=0,-999,ACC_C_WL_CCT/APP_C_WL_CCT) as float )       as PR_WL_CCT,
        |APP_C_UWL_CCT,
        |ACC_C_UWL_CCT,
        |cast(if(APP_C_UWL_CCT=0,-999,ACC_C_UWL_CCT/APP_C_UWL_CCT) as float )    as PR_UWL_CCT,
        |AF_ACC_C_CT,
        |cast(if(APP_C_CT=0,-999,AF_ACC_C_CT/APP_C_CT) as float )                as AF_PR_CT
        |from now_view1 join now_view2 on now_view1.BUSINESS_TYPE_CD=now_view2.BUSINESS_TYPE_CD
        |""".stripMargin
    tableEnv.sqlUpdate(nv3)

    val no_window_sink =
      """
        |   CREATE TABLE no_window_sink (
        |        BUSINESS_TYPE_CD string,
        |        ts bigint,
        |        APP_C_CT bigint,
        |        ACC_C_CT bigint,
        |        PR_CT float,
        |        APP_C_WL_CCT    bigint,
        |        ACC_C_WL_CCT    bigint,
        |        PR_WL_CCT float ,
        |        APP_C_UWL_CCT   bigint,
        |        ACC_C_UWL_CCT   bigint,
        |        PR_UWL_CCT float,
        |        AF_ACC_C_CT     bigint,
        |        AF_PR_CT    float
        |      )
        |       WITH (
        |      'connector.type' = 'kafka',
        |	  'connector.version' = 'universal',
        |      'connector.topic' = 'nowindow',
        |      'connector.properties.group.id'='dev_flink',
        |      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
        |      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
        |      'format.type' = 'json',
        |      'update-mode' = 'append'
        |    )
        |    """.stripMargin
    tableEnv.sqlUpdate(no_window_sink)

    tableEnv.sqlUpdate("insert into no_window_sink select * from now_view3")

    val no_window_source =
      """
        |    CREATE TABLE no_window_source (
        |        BUSINESS_TYPE_CD string,
        |        ts bigint,
        |        APP_C_CT bigint,
        |        ACC_C_CT bigint,
        |        PR_CT float,
        |        APP_C_WL_CCT    bigint,
        |        ACC_C_WL_CCT    bigint,
        |        PR_WL_CCT float ,
        |        APP_C_UWL_CCT   bigint,
        |        ACC_C_UWL_CCT   bigint,
        |        PR_UWL_CCT float,
        |        AF_ACC_C_CT     bigint,
        |        AF_PR_CT    float,
        |        et AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),
        |        WATERMARK FOR et AS et - INTERVAL '5' SECOND
        |    )
        |    WITH (
        |      'connector.type' = 'kafka',
        |      'connector.version' = 'universal',
        |      'connector.topic' = 'nowindow',
        |      'connector.properties.group.id'='dev_flink',
        |      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
        |      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
        |      'format.type' = 'json',
        |      'update-mode' = 'append',
        |      'connector.startup-mode' = 'latest-offset'
        |      )
        |      """.stripMargin
    tableEnv.sqlUpdate(no_window_source)

    val sink =
      """
        | CREATE TABLE kafka_product_monitor_suixindai (
        |    BUSINESS_TYPE_CD        STRING,
        |    BUSINESS_TYPE_NAME      STRING,
        |    DT                      TIMESTAMP(3),
        |    APP_C_CT                BIGINT,
        |    ACC_C_CT                BIGINT,
        |    PR_CT                   FLOAT,
        |    APP_C_WL_CCT            BIGINT,
        |    ACC_C_WL_CCT            BIGINT,
        |    PR_WL_CCT               FLOAT ,
        |    APP_C_UWL_CCT           BIGINT,
        |    ACC_C_UWL_CCT           BIGINT,
        |    PR_UWL_CCT              FLOAT,
        |    AF_ACC_C_CT             BIGINT,
        |    AF_PR_CT                FLOAT,
        |    APP_C_CT_1H	            BIGINT,
        |    APP_C_CT_2H	            BIGINT,
        |    APP_C_CT_6H	            BIGINT,
        |    ACC_C_CT_1H	            BIGINT,
        |    ACC_C_CT_2H	            BIGINT,
        |    ACC_C_CT_6H	            BIGINT,
        |    PR_CT_1H	            FLOAT,
        |    PR_CT_2H	            FLOAT,
        |    PR_CT_6H	            FLOAT,
        |    ACC_C_CCT_1H	        BIGINT,
        |    ACC_C_CCT_2H	        BIGINT,
        |    ACC_C_CCT_6H	        BIGINT,
        |    AVG_AC_CCT_1H	        DOUBLE,
        |    AVG_AC_CCT_2H	        DOUBLE,
        |    AVG_AC_CCT_6H	        DOUBLE,
        |    APP_C_WL_CCT_1H	        BIGINT,
        |    APP_C_WL_CCT_2H	        BIGINT,
        |    APP_C_WL_CCT_6H	        BIGINT,
        |    ACC_C_WL_CCT_1H	        BIGINT,
        |    ACC_C_WL_CCT_2H	        BIGINT,
        |    ACC_C_WL_CCT_6H	        BIGINT,
        |    PR_WL_CCT_1H	        FLOAT,
        |    PR_WL_CCT_2H	        FLOAT,
        |    PR_WL_CCT_6H	        FLOAT,
        |    APP_C_UWL_CCT_1H	    BIGINT,
        |    APP_C_UWL_CCT_2H	    BIGINT,
        |    APP_C_UWL_CCT_6H	    BIGINT,
        |    ACC_C_UWL_CCT_1H	    BIGINT,
        |    ACC_C_UWL_CCT_2H	    BIGINT,
        |    ACC_C_UWL_CCT_6H	    BIGINT,
        |    PR_UWL_CCT_1H	        FLOAT,
        |    PR_UWL_CCT_2H	        FLOAT,
        |    PR_UWL_CCT_6H	        FLOAT,
        |    AF_ACC_C_CT_1H	        BIGINT,
        |    AF_ACC_C_CT_2H	        BIGINT,
        |    AF_ACC_C_CT_6H	        BIGINT,
        |    AF_PR_CT_1H	            FLOAT,
        |    AF_PR_CT_2H	            FLOAT,
        |    AF_PR_CT_6H	            FLOAT,
        |    AVG_ACC_CCSCT_1H	    DOUBLE,
        |    AVG_ACC_CCSCT_2H	    DOUBLE,
        |    AVG_ACC_CCSCT_6H	    DOUBLE,
        |    AVG_ACC_PCSCT_1H	    DOUBLE,
        |    AVG_ACC_PCSCT_2H	    DOUBLE,
        |    AVG_ACC_PCSCT_6H	    DOUBLE,
        |    RATE_OC_1H	            DOUBLE,
        |    RATE_OC_2H	            DOUBLE,
        |    RATE_OC_6H	            DOUBLE
        |      )
        |       WITH (
        |      'connector.type' = 'kafka',
        |	  'connector.version' = 'universal',
        |      'connector.topic' = 'kafkaProductMonitor',
        |      'connector.properties.group.id'='dev_flink',
        |      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
        |      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
        |      'format.type' = 'json',
        |      'update-mode' = 'append'
        |    )""".stripMargin
    tableEnv.sqlUpdate(sink)

    val av1 =
      """
        |create view apply_view1 as
        |select
        |    BUSINESS_TYPE_CD,
        |    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR) AS dt,
        |    count (1) as APP_C_CT_1H
        |from kafka_apply_info_200 GROUP BY HOP(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR),BUSINESS_TYPE_CD
        |""".stripMargin
    tableEnv.sqlUpdate(av1)
    val av2 =
      """
        |create view apply_view2 as
        |select
        |BUSINESS_TYPE_CD,
        |    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '2' HOUR) AS dt,
        |    count (1) as APP_C_CT_2H
        |from kafka_apply_info_200 GROUP BY HOP(et,INTERVAL '10' SECOND,INTERVAL '2' HOUR),BUSINESS_TYPE_CD
        |""".stripMargin
    tableEnv.sqlUpdate(av2)
    val av3 =
      """
        |create view apply_view6 as
        |select
        |BUSINESS_TYPE_CD,
        |    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '6' HOUR) AS dt,
        |    count (1) as APP_C_CT_6H
        |from kafka_apply_info_200 GROUP BY HOP(et,INTERVAL '10' SECOND,INTERVAL '6' HOUR),BUSINESS_TYPE_CD
        |""".stripMargin
    tableEnv.sqlUpdate(av3)
    val mid1 =
      """
        |create view mid_1 as
        |select
        |BUSINESS_TYPE_CD,
        |    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR)                                                                                                          as dt,
        |    sum (if(CREDIT_CODE='0',1,0))                                                                                                                                   as ACC_C_CT_1H,         -- 1小时时间窗内的贷款授信申请通过笔数
        |    -- cast (sum (if(CREDIT_CODE='0',1,0))/count (SESSION_ID) as float)                                                                                             as PR_CT_1H,            -- 1小时时间窗内的贷款授信申请通过笔数/申请笔数
        |    customcount(CUST_ID,CREDIT_CODE)                                                                                                                                as ACC_C_CCT_1H,        -- 1小时时间窗内的贷款授信申请通过客户数                   自定义函数求 集合的个数
        |    -- cast (customcount(CUST_ID,CREDIT_CODE)/count (SESSION_ID) as double)                                                                                         as AVG_AC_CCT_1H,       -- 1小时时间窗内的贷款授信申请通过客户数/申请笔数
        |    sum (if(WHITE_LIST=1,1,0))                                                                                                                                      as APP_C_WL_CCT_1H,     -- 1小时时间窗内的贷款授信申请且命中白名单客户的笔数
        |    sum (if(CREDIT_CODE='0' and WHITE_LIST=1,1,0))                                                                                                                  as ACC_C_WL_CCT_1H,     -- 1小时时间窗内的贷款授信申请通过且命中白名单客户的笔数
        |    cast (if (sum(if(WHITE_LIST=1,1,0))=0,-999,sum (if(CREDIT_CODE='0' and WHITE_LIST=1,1,0))/sum (if(WHITE_LIST=1,1,0))) as float)                                 as PR_WL_CCT_1H,        -- 1小时时间窗内的贷款授信申请通过且命中白名单客户的笔数/申请且命中白名单客户的笔数
        |    sum (if(WHITE_LIST=0,1,0))                                                                                                                                      as APP_C_UWL_CCT_1H,    -- 1小时时间窗内的贷款授信申请且未命中白名单客户的笔数
        |    sum (if(CREDIT_CODE='0' and WHITE_LIST=0,1,0))                                                                                                                  as ACC_C_UWL_CCT_1H,    -- 1小时时间窗内的贷款授信申请通过且未命中白名单客户的笔数
        |    cast (if(sum(if(WHITE_LIST=0,1,0))=0,-999,sum (if(CREDIT_CODE='0' and WHITE_LIST=0,1,0))/sum(if(WHITE_LIST=0,1,0))) as float )                                  as PR_UWL_CCT_1H,       -- 1小时时间窗内的贷款授信申请通过且未命中白名单客户的笔数/申请且未命中白名单客户的笔数
        |    sum (if(BLACK_LIST=0,1,0))                                                                                                                                      as AF_ACC_C_CT_1H,      -- 1小时时间窗内的贷款授信申请反欺诈通过笔数
        |    -- cast (sum (if(BLACK_LIST='0',1,0))/count (SESSION_ID) as float)                                                                                              as AF_PR_CT_1H,         -- 1小时时间窗内的贷款授信申请反欺诈通过笔数/申请笔数
        |    cast ( if (sum (if(CREDIT_CODE='0',1,0))=0,-999,sum(if(CREDIT_CODE='0',CREDIT_SCORE_1,0))/sum (if(CREDIT_CODE='0',1,0))) as double )                            as AVG_ACC_CCSCT_1H,    -- 1小时时间窗内的贷款授信申请通过客户的客户卡评分均值           自定义函数
        |    cast ( if (sum (if(CREDIT_CODE='0',1,0))=0,-999,sum(if(CREDIT_CODE='0',APPLY_SCORE,0))/sum (if(CREDIT_CODE='0',1,0))) as double )                               as AVG_ACC_PCSCT_1H,    -- 1小时时间窗内的贷款授信申请通过客户的产品卡评分均值
        |    cast ( if(customcount(CUST_ID,CREDIT_CODE)=0,-999,customcount(CUST_ID,CREDIT_CODE,CAST(CREDIT_SCORE_1 AS INT),CREDIT_LIMIT)/customcount(CUST_ID,CREDIT_CODE)) as double)     as RATE_OC_1H           -- 1小时时间窗内的贷款授信申请通过客户的客户卡评分低于220分且额度高于4万元的客户数量/申请通过客户数量 自定义函数
        |from kafka_result_info_200 group by HOP(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR),BUSINESS_TYPE_CD""".stripMargin
    tableEnv.sqlUpdate(mid1)

    val mid1h =
      """
        |create view mid_1h as
        |select
        |mid_1.BUSINESS_TYPE_CD,
        |    apply_view1.dt,
        |    APP_C_CT_1H,
        |    ACC_C_CT_1H,
        |    if(APP_C_CT_1H=0,-999,cast (ACC_C_CT_1H/APP_C_CT_1H as float)) as PR_CT_1H,
        |    ACC_C_CCT_1H,
        |    if(APP_C_CT_1H=0,-999,cast (ACC_C_CCT_1H/APP_C_CT_1H as double )) as AVG_AC_CCT_1H,
        |    APP_C_WL_CCT_1H,
        |    ACC_C_WL_CCT_1H,
        |    PR_WL_CCT_1H,
        |    APP_C_UWL_CCT_1H,
        |    ACC_C_UWL_CCT_1H,
        |    PR_UWL_CCT_1H,
        |    AF_ACC_C_CT_1H,
        |    if(APP_C_CT_1H=0,-999,cast(AF_ACC_C_CT_1H/APP_C_CT_1H as float )) as AF_PR_CT_1H,
        |    AVG_ACC_CCSCT_1H,
        |    AVG_ACC_PCSCT_1H,
        |    RATE_OC_1H
        |from mid_1,apply_view1 where mid_1.BUSINESS_TYPE_CD=apply_view1.BUSINESS_TYPE_CD and mid_1.dt between apply_view1.dt - interval '2' second and apply_view1.dt + interval '2' second""".stripMargin
    tableEnv.sqlUpdate(mid1h)
    val mid2 =
      """
        |create view mid_2 as
        |select
        |BUSINESS_TYPE_CD,
        |    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '2' HOUR)                                                                                                          as dt,
        |    sum (if(CREDIT_CODE='0',1,0))                                                                                                                                   as ACC_C_CT_2H,         -- 2小时时间窗内的贷款授信申请通过笔数
        |    -- cast (sum (if(CREDIT_CODE='0',1,0))/count (SESSION_ID) as float)                                                                                             as PR_CT_2H,            -- 2小时时间窗内的贷款授信申请通过笔数/申请笔数
        |    customcount(CUST_ID,CREDIT_CODE)                                                                                                                                as ACC_C_CCT_2H,        -- 2小时时间窗内的贷款授信申请通过客户数                   自定义函数求 集合的个数
        |    -- cast (customcount(CUST_ID,CREDIT_CODE)/count (SESSION_ID) as double)                                                                                         as AVG_AC_CCT_2H,       -- 2小时时间窗内的贷款授信申请通过客户数/申请笔数
        |    sum (if(WHITE_LIST=1,1,0))                                                                                                                                      as APP_C_WL_CCT_2H,     -- 2小时时间窗内的贷款授信申请且命中白名单客户的笔数
        |    sum (if(CREDIT_CODE='0' and WHITE_LIST=1,1,0))                                                                                                                  as ACC_C_WL_CCT_2H,     -- 2小时时间窗内的贷款授信申请通过且命中白名单客户的笔数
        |    cast (if (sum(if(WHITE_LIST=1,1,0))=0,-999,sum (if(CREDIT_CODE='0' and WHITE_LIST=1,1,0))/sum (if(WHITE_LIST=1,1,0))) as float)                                 as PR_WL_CCT_2H,        -- 2小时时间窗内的贷款授信申请通过且命中白名单客户的笔数/申请且命中白名单客户的笔数
        |    sum (if(WHITE_LIST=0,1,0))                                                                                                                                      as APP_C_UWL_CCT_2H,    -- 2小时时间窗内的贷款授信申请且未命中白名单客户的笔数
        |    sum (if(CREDIT_CODE='0' and WHITE_LIST=0,1,0))                                                                                                                  as ACC_C_UWL_CCT_2H,    -- 2小时时间窗内的贷款授信申请通过且未命中白名单客户的笔数
        |    cast (if(sum(if(WHITE_LIST=0,1,0))=0,-999,sum (if(CREDIT_CODE='0' and WHITE_LIST=0,1,0))/sum(if(WHITE_LIST=0,1,0))) as float )                                  as PR_UWL_CCT_2H,       -- 2小时时间窗内的贷款授信申请通过且未命中白名单客户的笔数/申请且未命中白名单客户的笔数
        |    sum (if(BLACK_LIST=0,1,0))                                                                                                                                      as AF_ACC_C_CT_2H,      -- 2小时时间窗内的贷款授信申请反欺诈通过笔数
        |    -- cast (sum (if(BLACK_LIST='0',1,0))/count (SESSION_ID) as float)                                                                                              as AF_PR_CT_2H,         -- 2小时时间窗内的贷款授信申请反欺诈通过笔数/申请笔数
        |    cast ( if (sum (if(CREDIT_CODE='0',1,0))=0,-999,sum(if(CREDIT_CODE='0',CREDIT_SCORE_1,0))/sum (if(CREDIT_CODE='0',1,0))) as double )                            as AVG_ACC_CCSCT_2H,    -- 2小时时间窗内的贷款授信申请通过客户的客户卡评分均值           自定义函数
        |    cast ( if (sum (if(CREDIT_CODE='0',1,0))=0,-999,sum(if(CREDIT_CODE='0',APPLY_SCORE,0))/sum (if(CREDIT_CODE='0',1,0))) as double )                               as AVG_ACC_PCSCT_2H,    -- 2小时时间窗内的贷款授信申请通过客户的产品卡评分均值
        |    cast ( if(customcount(CUST_ID,CREDIT_CODE)=0,-999,customcount(CUST_ID,CREDIT_CODE,CAST(CREDIT_SCORE_1 AS INT),CREDIT_LIMIT)/customcount(CUST_ID,CREDIT_CODE)) as double)     as RATE_OC_2H           -- 2小时时间窗内的贷款授信申请通过客户的客户卡评分低于220分且额度高于4万元的客户数量/申请通过客户数量 自定义函数
        |from kafka_result_info_200 group by HOP(et,INTERVAL '10' SECOND,INTERVAL '2' HOUR),BUSINESS_TYPE_CD""".stripMargin
    tableEnv.sqlUpdate(mid2)

    val mid2h =
      """
        |create view mid_2h as
        |select
        |mid_2.BUSINESS_TYPE_CD,
        |    apply_view2.dt,
        |    APP_C_CT_2H,
        |    ACC_C_CT_2H,
        |    if(APP_C_CT_2H=0,-999,cast (ACC_C_CT_2H/APP_C_CT_2H as float)) as PR_CT_2H,
        |    ACC_C_CCT_2H,
        |    if(APP_C_CT_2H=0,-999,cast (ACC_C_CCT_2H/APP_C_CT_2H as double )) as AVG_AC_CCT_2H,
        |    APP_C_WL_CCT_2H,
        |    ACC_C_WL_CCT_2H,
        |    PR_WL_CCT_2H,
        |    APP_C_UWL_CCT_2H,
        |    ACC_C_UWL_CCT_2H,
        |    PR_UWL_CCT_2H,
        |    AF_ACC_C_CT_2H,
        |    if(APP_C_CT_2H=0,-999,cast(AF_ACC_C_CT_2H/APP_C_CT_2H as float )) as AF_PR_CT_2H,
        |    AVG_ACC_CCSCT_2H,
        |    AVG_ACC_PCSCT_2H,
        |    RATE_OC_2H
        |from mid_2,apply_view2 where mid_2.BUSINESS_TYPE_CD=apply_view2.BUSINESS_TYPE_CD and mid_2.dt between apply_view2.dt - interval '2' second and apply_view2.dt + interval '2' second
        |""".stripMargin
    tableEnv.sqlUpdate(mid2h)

    val mid6 =
      """
        |create view mid_6 as
        |select
        |BUSINESS_TYPE_CD,
        |    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '6' HOUR)                                                                                                          as dt,
        |    sum (if(CREDIT_CODE='0',1,0))                                                                                                                                   as ACC_C_CT_6H,         -- 6小时时间窗内的贷款授信申请通过笔数
        |    -- cast (sum (if(CREDIT_CODE='0',1,0))/count (SESSION_ID) as float)                                                                                             as PR_CT_6H,            -- 6小时时间窗内的贷款授信申请通过笔数/申请笔数
        |    customcount(CUST_ID,CREDIT_CODE)                                                                                                                                as ACC_C_CCT_6H,        -- 6小时时间窗内的贷款授信申请通过客户数                   自定义函数求 集合的个数
        |    -- cast (customcount(CUST_ID,CREDIT_CODE)/count (SESSION_ID) as double)                                                                                         as AVG_AC_CCT_6H,       -- 6小时时间窗内的贷款授信申请通过客户数/申请笔数
        |    sum (if(WHITE_LIST=1,1,0))                                                                                                                                      as APP_C_WL_CCT_6H,     -- 6小时时间窗内的贷款授信申请且命中白名单客户的笔数
        |    sum (if(CREDIT_CODE='0' and WHITE_LIST=1,1,0))                                                                                                                  as ACC_C_WL_CCT_6H,     -- 6小时时间窗内的贷款授信申请通过且命中白名单客户的笔数
        |    cast (if (sum(if(WHITE_LIST=1,1,0))=0,-999,sum (if(CREDIT_CODE='0' and WHITE_LIST=1,1,0))/sum (if(WHITE_LIST=1,1,0))) as float)                                 as PR_WL_CCT_6H,        -- 6小时时间窗内的贷款授信申请通过且命中白名单客户的笔数/申请且命中白名单客户的笔数
        |    sum (if(WHITE_LIST=0,1,0))                                                                                                                                      as APP_C_UWL_CCT_6H,    -- 6小时时间窗内的贷款授信申请且未命中白名单客户的笔数
        |    sum (if(CREDIT_CODE='0' and WHITE_LIST=0,1,0))                                                                                                                  as ACC_C_UWL_CCT_6H,    -- 6小时时间窗内的贷款授信申请通过且未命中白名单客户的笔数
        |    cast (if(sum(if(WHITE_LIST=0,1,0))=0,-999,sum (if(CREDIT_CODE='0' and WHITE_LIST=0,1,0))/sum(if(WHITE_LIST=0,1,0))) as float )                                  as PR_UWL_CCT_6H,       -- 6小时时间窗内的贷款授信申请通过且未命中白名单客户的笔数/申请且未命中白名单客户的笔数
        |    sum (if(BLACK_LIST=0,1,0))                                                                                                                                      as AF_ACC_C_CT_6H,      -- 6小时时间窗内的贷款授信申请反欺诈通过笔数
        |    -- cast (sum (if(BLACK_LIST='0',1,0))/count (SESSION_ID) as float)                                                                                              as AF_PR_CT_6H,         -- 6小时时间窗内的贷款授信申请反欺诈通过笔数/申请笔数
        |    cast ( if (sum (if(CREDIT_CODE='0',1,0))=0,-999,sum(if(CREDIT_CODE='0',CREDIT_SCORE_1,0))/sum (if(CREDIT_CODE='0',1,0))) as double )                            as AVG_ACC_CCSCT_6H,    -- 6小时时间窗内的贷款授信申请通过客户的客户卡评分均值           自定义函数
        |    cast ( if (sum (if(CREDIT_CODE='0',1,0))=0,-999,sum(if(CREDIT_CODE='0',APPLY_SCORE,0))/sum (if(CREDIT_CODE='0',1,0))) as double )                               as AVG_ACC_PCSCT_6H,    -- 6小时时间窗内的贷款授信申请通过客户的产品卡评分均值
        |    cast ( if(customcount(CUST_ID,CREDIT_CODE)=0,-999,customcount(CUST_ID,CREDIT_CODE,CAST(CREDIT_SCORE_1 AS INT),CREDIT_LIMIT)/customcount(CUST_ID,CREDIT_CODE)) as double)     as RATE_OC_6H           -- 6小时时间窗内的贷款授信申请通过客户的客户卡评分低于220分且额度高于4万元的客户数量/申请通过客户数量 自定义函数
        |from kafka_result_info_200 group by HOP(et,INTERVAL '10' SECOND,INTERVAL '6' HOUR),BUSINESS_TYPE_CD""".stripMargin
    tableEnv.sqlUpdate(mid6)

    val mid6h =
      """
        |create view mid_6h as
        |select
        |mid_6.BUSINESS_TYPE_CD,
        |    apply_view6.dt,
        |    APP_C_CT_6H,
        |    ACC_C_CT_6H,
        |    if(APP_C_CT_6H=0,-999,cast (ACC_C_CT_6H/APP_C_CT_6H as float)) as PR_CT_6H,
        |    ACC_C_CCT_6H,
        |    if(APP_C_CT_6H=0,-999,cast (ACC_C_CCT_6H/APP_C_CT_6H as double )) as AVG_AC_CCT_6H,
        |    APP_C_WL_CCT_6H,
        |    ACC_C_WL_CCT_6H,
        |    PR_WL_CCT_6H,
        |    APP_C_UWL_CCT_6H,
        |    ACC_C_UWL_CCT_6H,
        |    PR_UWL_CCT_6H,
        |    AF_ACC_C_CT_6H,
        |    if(APP_C_CT_6H=0,-999,cast(AF_ACC_C_CT_6H/APP_C_CT_6H as float )) as AF_PR_CT_6H,
        |    AVG_ACC_CCSCT_6H,
        |    AVG_ACC_PCSCT_6H,
        |    RATE_OC_6H
        |from mid_6,apply_view6 where mid_6.BUSINESS_TYPE_CD=apply_view6.BUSINESS_TYPE_CD and mid_6.dt between apply_view6.dt - interval '2' second and apply_view6.dt + interval '2' second""".stripMargin
    tableEnv.sqlUpdate(mid6h)

    val mt1 =
      """
        |create view mid_table1 as
        |select
        |    APP_C_CT_1H,
        |    ACC_C_CT_1H,
        |    PR_CT_1H,
        |    ACC_C_CCT_1H,
        |    AVG_AC_CCT_1H,
        |    APP_C_WL_CCT_1H,
        |    ACC_C_WL_CCT_1H,
        |    PR_WL_CCT_1H,
        |    APP_C_UWL_CCT_1H,
        |    ACC_C_UWL_CCT_1H,
        |    PR_UWL_CCT_1H,
        |    AF_ACC_C_CT_1H,
        |    AF_PR_CT_1H,
        |    AVG_ACC_CCSCT_1H,
        |    AVG_ACC_PCSCT_1H,
        |    RATE_OC_1H,
        |    mid_2h.*
        |from mid_1h,mid_2h where mid_1h.BUSINESS_TYPE_CD=mid_2h.BUSINESS_TYPE_CD and mid_1h.dt between mid_2h.dt - interval '2' second and mid_2h.dt + interval '2' second""".stripMargin
    tableEnv.sqlUpdate(mt1)

    val mt2 =
      """
        |create view mid_table2 as
        |select
        |    APP_C_CT_6H,
        |    ACC_C_CT_6H,
        |    PR_CT_6H,
        |    ACC_C_CCT_6H,
        |    AVG_AC_CCT_6H,
        |    APP_C_WL_CCT_6H,
        |    ACC_C_WL_CCT_6H,
        |    PR_WL_CCT_6H,
        |    APP_C_UWL_CCT_6H,
        |    ACC_C_UWL_CCT_6H,
        |    PR_UWL_CCT_6H,
        |    AF_ACC_C_CT_6H,
        |    AF_PR_CT_6H,
        |    AVG_ACC_CCSCT_6H,
        |    AVG_ACC_PCSCT_6H,
        |    RATE_OC_6H,
        |    mid_table1.*
        |from mid_table1,mid_6h where mid_table1.BUSINESS_TYPE_CD=mid_6h.BUSINESS_TYPE_CD and mid_table1.dt between mid_6h.dt - interval '2' second and mid_6h.dt + interval '2' second""".stripMargin
    tableEnv.sqlUpdate(mt2)

    val fv =
      """
        |create view final_sink as
        |select
        |    a.BUSINESS_TYPE_CD,
        |    if(a.BUSINESS_TYPE_CD='suixindai','自定义的business_name','xxx'),
        |    a.et,
        |    APP_C_CT,
        |    ACC_C_CT,
        |    PR_CT,
        |    APP_C_WL_CCT,
        |    ACC_C_WL_CCT,
        |    PR_WL_CCT,
        |    APP_C_UWL_CCT,
        |    ACC_C_UWL_CCT,
        |    PR_UWL_CCT,
        |    AF_ACC_C_CT,
        |    AF_PR_CT,
        |    APP_C_CT_1H,
        |    APP_C_CT_2H,
        |    APP_C_CT_6H,
        |    ACC_C_CT_1H,
        |    ACC_C_CT_2H,
        |    ACC_C_CT_6H,
        |    PR_CT_1H,
        |    PR_CT_2H,
        |    PR_CT_6H,
        |    ACC_C_CCT_1H,
        |    ACC_C_CCT_2H,
        |    ACC_C_CCT_6H,
        |    AVG_AC_CCT_1H,
        |    AVG_AC_CCT_2H,
        |    AVG_AC_CCT_6H,
        |    APP_C_WL_CCT_1H,
        |    APP_C_WL_CCT_2H,
        |    APP_C_WL_CCT_6H,
        |    ACC_C_WL_CCT_1H,
        |    ACC_C_WL_CCT_2H,
        |    ACC_C_WL_CCT_6H,
        |    PR_WL_CCT_1H,
        |    PR_WL_CCT_2H,
        |    PR_WL_CCT_6H,
        |    APP_C_UWL_CCT_1H,
        |    APP_C_UWL_CCT_2H,
        |    APP_C_UWL_CCT_6H,
        |    ACC_C_UWL_CCT_1H,
        |    ACC_C_UWL_CCT_2H,
        |    ACC_C_UWL_CCT_6H,
        |    PR_UWL_CCT_1H,
        |    PR_UWL_CCT_2H,
        |    PR_UWL_CCT_6H,
        |    AF_ACC_C_CT_1H,
        |    AF_ACC_C_CT_2H,
        |    AF_ACC_C_CT_6H,
        |    AF_PR_CT_1H,
        |    AF_PR_CT_2H,
        |    AF_PR_CT_6H,
        |    AVG_ACC_CCSCT_1H,
        |    AVG_ACC_CCSCT_2H,
        |    AVG_ACC_CCSCT_6H,
        |    AVG_ACC_PCSCT_1H,
        |    AVG_ACC_PCSCT_2H,
        |    AVG_ACC_PCSCT_6H,
        |    RATE_OC_1H,
        |    RATE_OC_2H,
        |    RATE_OC_6H
        |from no_window_source a join mid_table2 b on a.BUSINESS_TYPE_CD=b.BUSINESS_TYPE_CD and a.et between b.dt - interval '10' second and b.dt""".stripMargin
    tableEnv.sqlUpdate(fv)

    tableEnv.sqlUpdate("insert into kafka_product_monitor_suixindai select * from final_sink")

  }
}
