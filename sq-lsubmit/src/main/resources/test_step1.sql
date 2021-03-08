
-- 测试ok 15点57分

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
    count (1) as APP_C_CT,
    max (et) as et
from kafka_apply_info_200 where BUSINESS_TYPE_CD='suixindai' group by BUSINESS_TYPE_CD;


create view now_view2 as
select
    max (et) as et,
    BUSINESS_TYPE_CD,
    sum (if(CREDIT_CODE='0',1,0))                       as      ACC_C_CT,       -- 截止当前时点前贷款授信申请通过笔数
    sum (if(WHITE_LIST=1,1,0))                          as      APP_C_WL_CCT,   -- 截止当前时点前贷款授信申请且命中白名单客户的笔数
    sum (if(CREDIT_CODE='0' and WHITE_LIST=1,1,0))      as      ACC_C_WL_CCT,   -- 截止当前时点前贷款授信申请通过且命中白名单客户的笔数
    sum (if(WHITE_LIST=0,1,0))                          as      APP_C_UWL_CCT,  -- 截止当前时点前贷款授信申请且wei命中白名单客户的笔数
    sum (if(CREDIT_CODE='0' and WHITE_LIST=0,1,0))      as      ACC_C_UWL_CCT,  -- 截止当前时点前贷款授信申请通过且wei命中白名单客户的笔数
    sum (if(BLACK_LIST=1,1,0))                          as      AF_ACC_C_CT,    -- 截止当前时点前贷款授信申请反欺诈通过笔数
    avg (CREDIT_SCORE_1)                                as      AVG_ACC_CCSCT,  -- 截止当前时点前贷款授信申请通过客户的客户卡评分均值
    avg(APPLY_SCORE)                                    as      AVG_ACC_PCSCT   -- 截止当前时点前贷款授信申请通过客户的产品卡评分均值
from kafka_result_info_200 where BUSINESS_TYPE_CD='suixindai' group by BUSINESS_TYPE_CD;


create view now_view3 as
select
    now_view1.BUSINESS_TYPE_CD,
    now_view1.et,
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
from now_view1 join now_view2 on now_view1.BUSINESS_TYPE_CD=now_view2.BUSINESS_TYPE_CD and now_view1.et between now_view2.et - interval '2' second and now_view2.et+interval '2' second;


create view apply_view1 as
select
    BUSINESS_TYPE_CD,
    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR) AS dt,
    count (1) as APP_C_CT_1H
from kafka_apply_info_200 GROUP BY HOP(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR),BUSINESS_TYPE_CD;


create view view4 as
select
now_view3.et,
now_view3.APP_C_CT,
apply_view1.APP_C_CT_1H
from now_view3 join apply_view1 on now_view3.BUSINESS_TYPE_CD=apply_view1.BUSINESS_TYPE_CD and now_view3.et between apply_view1.dt - interval '2' second and apply_view1.dt+interval '2' second;



-- 先把结果插入kafka再读，为什么这么做？因为再view中没办法指定时间属性，必须有时间属性才能和开窗聚合的几个属性进行interval join
   CREATE TABLE no_window_sink (
        ts TIMESTAMP(3),
        APP_C_CT bigint,
        APP_C_CT_1H bigint
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
insert into no_window_sink select * from view4;



