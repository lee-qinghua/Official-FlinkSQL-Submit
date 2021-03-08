--

--kafka授信申请表
    CREATE TABLE kafka_apply_info_200 (
      SESSION_ID STRING,
      APP_NO STRING,
      CUST_ID STRING,
      BUSINESS_TYPE_CD STRING,
      BUSINESS_TYPE_NAME STRING,
      CUST_SOURCE STRING,
      CHANNEL_SOURCE STRING,
      APPLY_TIME BIGINT,
      et AS TO_TIMESTAMP(FROM_UNIXTIME(APPLY_TIME/1000)),
      WATERMARK FOR et AS et - INTERVAL '5' SECOND
    )
    WITH (
      'connector.type' = 'kafka',
      'connector.version' = 'universal',
      'connector.topic' = 'kafkaCreditApplyInfo_1234',
      'connector.properties.group.id'='dev_flink',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
      'format.type' = 'json',
      'update-mode' = 'append',
      'connector.startup-mode' = 'latest-offset'
      );


-- create view now_view1 as
-- select
--     BUSINESS_TYPE_CD,
--     count (1) as APP_C_CT,
--     max (APPLY_TIME) as ts
-- from kafka_apply_info_200 where BUSINESS_TYPE_CD='suixindai' group by BUSINESS_TYPE_CD;
--
--
-- create view now_view2 as
-- select
--     BUSINESS_TYPE_CD,
--     sum (if(CREDIT_CODE='0',1,0))                       as      ACC_C_CT,       -- 截止当前时点前贷款授信申请通过笔数
--     sum (if(WHITE_LIST=1,1,0))                          as      APP_C_WL_CCT,   -- 截止当前时点前贷款授信申请且命中白名单客户的笔数
--     sum (if(CREDIT_CODE='0' and WHITE_LIST=1,1,0))      as      ACC_C_WL_CCT,   -- 截止当前时点前贷款授信申请通过且命中白名单客户的笔数
--     sum (if(WHITE_LIST=0,1,0))                          as      APP_C_UWL_CCT,  -- 截止当前时点前贷款授信申请且wei命中白名单客户的笔数
--     sum (if(CREDIT_CODE='0' and WHITE_LIST=0,1,0))      as      ACC_C_UWL_CCT,  -- 截止当前时点前贷款授信申请通过且wei命中白名单客户的笔数
--     sum (if(BLACK_LIST=1,1,0))                          as      AF_ACC_C_CT,    -- 截止当前时点前贷款授信申请反欺诈通过笔数
--     avg (CREDIT_SCORE_1)                                as      AVG_ACC_CCSCT,  -- 截止当前时点前贷款授信申请通过客户的客户卡评分均值
--     avg(APPLY_SCORE)                                    as      AVG_ACC_PCSCT   -- 截止当前时点前贷款授信申请通过客户的产品卡评分均值
-- from kafka_result_info_200 where BUSINESS_TYPE_CD='suixindai' group by BUSINESS_TYPE_CD;
--
--
-- create view now_view3 as
-- select
--     now_view1.BUSINESS_TYPE_CD,
--     ts,
--     APP_C_CT,
--     ACC_C_CT,
--     cast(if(APP_C_CT=0,-999,ACC_C_CT/APP_C_CT) as float)                    as PR_CT,
--     APP_C_WL_CCT,
--     ACC_C_WL_CCT,
--     cast(if(APP_C_WL_CCT=0,-999,ACC_C_WL_CCT/APP_C_WL_CCT) as float )       as PR_WL_CCT,
--     APP_C_UWL_CCT,
--     ACC_C_UWL_CCT,
--     cast(if(APP_C_UWL_CCT=0,-999,ACC_C_UWL_CCT/APP_C_UWL_CCT) as float )    as PR_UWL_CCT,
--     AF_ACC_C_CT,
--     cast(if(APP_C_CT=0,-999,AF_ACC_C_CT/APP_C_CT) as float )                as AF_PR_CT
-- from now_view1 join now_view2 on now_view1.BUSINESS_TYPE_CD=now_view2.BUSINESS_TYPE_CD;

-- -- 先把结果插入kafka再读，为什么这么做？因为再view中没办法指定时间属性，必须有时间属性才能和开窗聚合的几个属性进行interval join
--    CREATE TABLE no_window_sink (
--         BUSINESS_TYPE_CD string,
--         ts bigint,
--         APP_C_CT bigint,
--         ACC_C_CT bigint,
--         PR_CT float,
--         APP_C_WL_CCT    bigint,
--         ACC_C_WL_CCT    bigint,
--         PR_WL_CCT float ,
--         APP_C_UWL_CCT   bigint,
--         ACC_C_UWL_CCT   bigint,
--         PR_UWL_CCT float,
--         AF_ACC_C_CT     bigint,
--         AF_PR_CT    float
--       )
--        WITH (
--       'connector.type' = 'kafka',
-- 	  'connector.version' = 'universal',
--       'connector.topic' = 'nowindow',
--       'connector.properties.group.id'='dev_flink',
--       'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
--       'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
--       'format.type' = 'json',
--       'update-mode' = 'append'
--     );
-- insert into no_window_sink select * from now_view3;
--
--
-- --  读取上一步的结果设置时间属性
--     CREATE TABLE no_window_source (
--         BUSINESS_TYPE_CD string,
--         ts bigint,
--         APP_C_CT bigint,
--         ACC_C_CT bigint,
--         PR_CT float,
--         APP_C_WL_CCT    bigint,
--         ACC_C_WL_CCT    bigint,
--         PR_WL_CCT float ,
--         APP_C_UWL_CCT   bigint,
--         ACC_C_UWL_CCT   bigint,
--         PR_UWL_CCT float,
--         AF_ACC_C_CT     bigint,
--         AF_PR_CT    float,
--         et AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),
--         WATERMARK FOR et AS et - INTERVAL '5' SECOND
--     )
--     WITH (
--       'connector.type' = 'kafka',
--       'connector.version' = 'universal',
--       'connector.topic' = 'nowindow',
--       'connector.properties.group.id'='dev_flink',
--       'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
--       'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
--       'format.type' = 'json',
--       'update-mode' = 'append',
--       'connector.startup-mode' = 'latest-offset'
--       );


-- 1小时申请表
create view apply_view1 as
select
    BUSINESS_TYPE_CD,
    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR) AS dt,
    count (1) as APP_C_CT_1H
from kafka_apply_info_200 GROUP BY HOP(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR),BUSINESS_TYPE_CD;


-- 移动测试
   CREATE TABLE test (
        BUSINESS_TYPE_CD string
      )
       WITH (
      'connector.type' = 'kafka',
	  'connector.version' = 'universal',
      'connector.topic' = 'test',
      'connector.properties.group.id'='dev_flink',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
      'format.type' = 'json',
      'update-mode' = 'append'
    );

insert into test
select
    BUSINESS_TYPE_CD
from kafka_apply_info_200;