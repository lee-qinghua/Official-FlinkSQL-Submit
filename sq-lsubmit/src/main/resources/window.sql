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

--kafka授信结果表
   CREATE TABLE kafka_result_info_200 (
      SESSION_ID STRING,
      APP_NO  STRING,
      CUST_ID STRING,
      CREDIT_NO  STRING,
      BUSINESS_TYPE_CD STRING,
      STATE_CODE STRING,
      CREDIT_CODE  STRING,
      REFUSE_REASON STRING,
      INTEREST_RATE  DOUBLE,
      CREDIT_LIMIT DOUBLE,
     REPAY_MODE_CD STRING,
     LOAN_TERM INTEGER,
     CREDIT_SCORE_1  DOUBLE, -- 客户卡评分
     CREDIT_SCORE_2 DOUBLE,
     CREDIT_SCORE_3  DOUBLE,
     ANTI_FRAUD_SCORE_1  DOUBLE,
     ANTI_FRAUD_SCORE_2  DOUBLE,
     WHITE_LIST int, -- 命中白名单标识 1 命中 0 未命中
     APPLY_SCORE int,--产品卡评分
     BLACK_LIST int, -- 命中黑名单标识 1 命中 0 未命中
     CREDIT_TIME  BIGINT,
     et AS TO_TIMESTAMP(FROM_UNIXTIME(CREDIT_TIME/1000)),
    WATERMARK FOR et AS et - INTERVAL '5' SECOND
    )WITH (
      'connector.type' = 'kafka',
      'connector.version' = 'universal',
      'connector.topic' = 'kafkaCreditResultInfo_1234',
      'connector.properties.group.id'='dev_flink',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
      'format.type' = 'json',
      'update-mode' = 'append',
      'connector.startup-mode' = 'latest-offset'
    );

create view now_view1 as
select
    BUSINESS_TYPE_CD,
    count (1) over w as APP_C_CT,
    MAX(SUBSTR(DATE_FORMAT(et, 'yyyy-MM-dd HH:mm:ss'),1,18) || '0') OVER w AS time_str,
    et
from  kafka_apply_info_200 where BUSINESS_TYPE_CD='suixindai'
window w as (order by et ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);


create view now_view2 as
select
    et,
    MAX(SUBSTR(DATE_FORMAT(et, 'yyyy-MM-dd HH:mm:ss'),1,18) || '0') OVER w AS time_str,
    BUSINESS_TYPE_CD,
    sum (if(CREDIT_CODE='0',1,0))                       over w as      ACC_C_CT,       -- 截止当前时点前贷款授信申请通过笔数
    sum (if(WHITE_LIST=1,1,0))                          over w as      APP_C_WL_CCT,   -- 截止当前时点前贷款授信申请且命中白名单客户的笔数
    sum (if(CREDIT_CODE='0' and WHITE_LIST=1,1,0))      over w as      ACC_C_WL_CCT,   -- 截止当前时点前贷款授信申请通过且命中白名单客户的笔数
    sum (if(WHITE_LIST=0,1,0))                          over w as      APP_C_UWL_CCT,  -- 截止当前时点前贷款授信申请且wei命中白名单客户的笔数
    sum (if(CREDIT_CODE='0' and WHITE_LIST=0,1,0))      over w as      ACC_C_UWL_CCT,  -- 截止当前时点前贷款授信申请通过且wei命中白名单客户的笔数
    sum (if(BLACK_LIST=1,1,0))                          over w as      AF_ACC_C_CT,    -- 截止当前时点前贷款授信申请反欺诈通过笔数
    avg (CREDIT_SCORE_1)                                over w as      AVG_ACC_CCSCT,  -- 截止当前时点前贷款授信申请通过客户的客户卡评分均值
    avg(APPLY_SCORE)                                    over w as      AVG_ACC_PCSCT   -- 截止当前时点前贷款授信申请通过客户的产品卡评分均值
from kafka_result_info_200 where BUSINESS_TYPE_CD='suixindai' window w as (order by et ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);


create view now_view1_10 as
    select
    time_str,
    max(et) as et,
    BUSINESS_TYPE_CD,
    max(APP_C_CT) as APP_C_CT
from now_view1 group by BUSINESS_TYPE_CD,time_str;


create view now_view2_10 as
    select
    time_str,
    max(et) as et,
    BUSINESS_TYPE_CD,
    max(ACC_C_CT) as ACC_C_CT,
    max(APP_C_WL_CCT) as APP_C_WL_CCT,
    max(ACC_C_WL_CCT) as ACC_C_WL_CCT,
    max(APP_C_UWL_CCT) as APP_C_UWL_CCT,
    max(ACC_C_UWL_CCT) as ACC_C_UWL_CCT,
    max(AF_ACC_C_CT) as AF_ACC_C_CT,
    max(AVG_ACC_CCSCT) as AVG_ACC_CCSCT,
    max(AVG_ACC_PCSCT) as AVG_ACC_PCSCT
from now_view2 group by BUSINESS_TYPE_CD,time_str;
-- create view now_view2_10 as
-- select
-- *
-- from
-- (select
--     et,
--     BUSINESS_TYPE_CD,
--     time_str,
--     ACC_C_CT,
--     APP_C_WL_CCT,
--     ACC_C_WL_CCT,
--     APP_C_UWL_CCT,
--     ACC_C_UWL_CCT,
--     AF_ACC_C_CT,
--     AVG_ACC_CCSCT,
--     AVG_ACC_PCSCT
-- row_number() over(partition by time_str,BUSINESS_TYPE_CD order by et desc )as rn from now_view2)t where rn=1;

-- 合并两个流
create view now_view3 as
select
a.et,
a.BUSINESS_TYPE_CD,
a.ACC_C_CT,
b.APP_C_CT
from now_view2_10 a join now_view1_10 b on a.BUSINESS_TYPE_CD=b.BUSINESS_TYPE_CD and a.time_str=b.time_str and a.et between b.et-interval '2' second and b.et+interval '2' second;


=====================================================================================================================================================================================
create view now_view3 as
select
    now_view1.BUSINESS_TYPE_CD,
    ts,
    APP_C_CT,
    ACC_C_CT,
    cast(if(APP_C_CT=0,-999,ACC_C_CT/APP_C_CT) as float)                    as PR_CT,
    APP_C_WL_CCT,
    ACC_C_WL_CCT,
    cast(if(APP_C_WL_CCT=0,-999,ACC_C_WL_CCT/APP_C_WL_CCT) as float )       as PR_WL_CCT,
    APP_C_UWL_CCT,
    ACC_C_UWL_CCT,
    cast(if(APP_C_UWL_CCT=0,-999,ACC_C_UWL_CCT/APP_C_UWL_CCT) as float )    as PR_UWL_CCT,
    AF_ACC_C_CT,
    cast(if(APP_C_CT=0,-999,AF_ACC_C_CT/APP_C_CT) as float )                as AF_PR_CT
from now_view1 join now_view2 on now_view1.BUSINESS_TYPE_CD=now_view2.BUSINESS_TYPE_CD;

-- 先把结果插入kafka再读，为什么这么做？因为再view中没办法指定时间属性，必须有时间属性才能和开窗聚合的几个属性进行interval join
   CREATE TABLE no_window_sink (
        BUSINESS_TYPE_CD string,
        ts bigint,
        APP_C_CT bigint,
        ACC_C_CT bigint,
        PR_CT float,
        APP_C_WL_CCT    bigint,
        ACC_C_WL_CCT    bigint,
        PR_WL_CCT float ,
        APP_C_UWL_CCT   bigint,
        ACC_C_UWL_CCT   bigint,
        PR_UWL_CCT float,
        AF_ACC_C_CT     bigint,
        AF_PR_CT    float
      )
       WITH (
      'connector.type' = 'kafka',
	  'connector.version' = 'universal',
      'connector.topic' = 'nowindow',
      'connector.properties.group.id'='dev_flink',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
      'format.type' = 'json',
      'update-mode' = 'append'
    );
insert into no_window_sink select * from now_view3;


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




