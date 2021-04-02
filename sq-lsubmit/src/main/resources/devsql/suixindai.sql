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
      'connector.properties.group.id'='dev_flink2',
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
      'connector.properties.group.id'='dev_flink2',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
      'format.type' = 'json',
      'update-mode' = 'append',
      'connector.startup-mode' = 'latest-offset'
    );

--kafka sink表
   CREATE TABLE kafka_product_monitor_suixindai (
    BUSINESS_TYPE_CD        STRING,
    BUSINESS_TYPE_NAME      STRING,
    DT                      TIMESTAMP(3),
    APP_C_CT_1H	            BIGINT,
    APP_C_CT_2H	            BIGINT,
    APP_C_CT_6H	            BIGINT,
    ACC_C_CT_1H	            BIGINT,
    ACC_C_CT_2H	            BIGINT,
    ACC_C_CT_6H	            BIGINT,
    PR_CT_1H	            FLOAT,
    PR_CT_2H	            FLOAT,
    PR_CT_6H	            FLOAT,
    ACC_C_CCT_1H	        BIGINT,
    ACC_C_CCT_2H	        BIGINT,
    ACC_C_CCT_6H	        BIGINT,
    AVG_AC_CCT_1H	        DOUBLE,
    AVG_AC_CCT_2H	        DOUBLE,
    AVG_AC_CCT_6H	        DOUBLE,
    APP_C_WL_CCT_1H	        BIGINT,
    APP_C_WL_CCT_2H	        BIGINT,
    APP_C_WL_CCT_6H	        BIGINT,
    ACC_C_WL_CCT_1H	        BIGINT,
    ACC_C_WL_CCT_2H	        BIGINT,
    ACC_C_WL_CCT_6H	        BIGINT,
    PR_WL_CCT_1H	        FLOAT,
    PR_WL_CCT_2H	        FLOAT,
    PR_WL_CCT_6H	        FLOAT,
    APP_C_UWL_CCT_1H	    BIGINT,
    APP_C_UWL_CCT_2H	    BIGINT,
    APP_C_UWL_CCT_6H	    BIGINT,
    ACC_C_UWL_CCT_1H	    BIGINT,
    ACC_C_UWL_CCT_2H	    BIGINT,
    ACC_C_UWL_CCT_6H	    BIGINT,
    PR_UWL_CCT_1H	        FLOAT,
    PR_UWL_CCT_2H	        FLOAT,
    PR_UWL_CCT_6H	        FLOAT,
    AF_ACC_C_CT_1H	        BIGINT,
    AF_ACC_C_CT_2H	        BIGINT,
    AF_ACC_C_CT_6H	        BIGINT,
    AF_PR_CT_1H	            FLOAT,
    AF_PR_CT_2H	            FLOAT,
    AF_PR_CT_6H	            FLOAT,
    AVG_ACC_CCSCT_1H	    DOUBLE,
    AVG_ACC_CCSCT_2H	    DOUBLE,
    AVG_ACC_CCSCT_6H	    DOUBLE,
    AVG_ACC_PCSCT_1H	    DOUBLE,
    AVG_ACC_PCSCT_2H	    DOUBLE,
    AVG_ACC_PCSCT_6H	    DOUBLE,
    RATE_OC_1H	            DOUBLE,
    RATE_OC_2H	            DOUBLE,
    RATE_OC_6H	            DOUBLE
      )
       WITH (
      'connector.type' = 'kafka',
	  'connector.version' = 'universal',
      'connector.topic' = 'suixindai_monitor',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
      'format.type' = 'json',
      'update-mode' = 'append'
    );

-- only suixindai
-- create view kafka_apply_info_200 as
-- select * from kafka_apply_info where BUSINESS_TYPE_CD='P0036';
--
-- create view kafka_result_info_200 as
-- select * from kafka_result_info where BUSINESS_TYPE_CD='P0036';



-- 1小时申请表
create view apply_view1 as
select
    BUSINESS_TYPE_CD,
    BUSINESS_TYPE_NAME,
    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR) AS dt,
    count (1) as APP_C_CT_1H
from kafka_apply_info_200 GROUP BY HOP(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR),BUSINESS_TYPE_CD,BUSINESS_TYPE_NAME;
-- 2小时申请表
create view apply_view2 as
select
BUSINESS_TYPE_CD,
    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '2' HOUR) AS dt,
    count (1) as APP_C_CT_2H
from kafka_apply_info_200 GROUP BY HOP(et,INTERVAL '10' SECOND,INTERVAL '2' HOUR),BUSINESS_TYPE_CD;
-- 3小时申请表
create view apply_view6 as
select
BUSINESS_TYPE_CD,
    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '6' HOUR) AS dt,
    count (1) as APP_C_CT_6H
from kafka_apply_info_200 GROUP BY HOP(et,INTERVAL '10' SECOND,INTERVAL '6' HOUR),BUSINESS_TYPE_CD;


    -- 一个小时窗口
create view mid_1 as
select
BUSINESS_TYPE_CD,
    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR)                                                                                                          as dt,
    sum (if(CREDIT_CODE='0',1,0))                                                                                                                                   as ACC_C_CT_1H,         -- 1小时时间窗内的贷款授信申请通过笔数
    -- cast (sum (if(CREDIT_CODE='0',1,0))/count (SESSION_ID) as float)                                                                                             as PR_CT_1H,            -- 1小时时间窗内的贷款授信申请通过笔数/申请笔数
    customcount(CUST_ID,CREDIT_CODE)                                                                                                                                as ACC_C_CCT_1H,        -- 1小时时间窗内的贷款授信申请通过客户数                   自定义函数求 集合的个数
    -- cast (customcount(CUST_ID,CREDIT_CODE)/count (SESSION_ID) as double)                                                                                         as AVG_AC_CCT_1H,       -- 1小时时间窗内的贷款授信申请通过客户数/申请笔数
    sum (if(WHITE_LIST=1,1,0))                                                                                                                                      as APP_C_WL_CCT_1H,     -- 1小时时间窗内的贷款授信申请且命中白名单客户的笔数
    sum (if(CREDIT_CODE='0' and WHITE_LIST=1,1,0))                                                                                                                  as ACC_C_WL_CCT_1H,     -- 1小时时间窗内的贷款授信申请通过且命中白名单客户的笔数
    cast (if (sum(if(WHITE_LIST=1,1,0))=0,-999,cast (sum (if(CREDIT_CODE='0' and WHITE_LIST=1,1,0)) as float )/sum (if(WHITE_LIST=1,1,0))) as float)                                 as PR_WL_CCT_1H,        -- 1小时时间窗内的贷款授信申请通过且命中白名单客户的笔数/申请且命中白名单客户的笔数
    sum (if(WHITE_LIST=0,1,0))                                                                                                                                      as APP_C_UWL_CCT_1H,    -- 1小时时间窗内的贷款授信申请且未命中白名单客户的笔数
    sum (if(CREDIT_CODE='0' and WHITE_LIST=0,1,0))                                                                                                                  as ACC_C_UWL_CCT_1H,    -- 1小时时间窗内的贷款授信申请通过且未命中白名单客户的笔数
    cast (if(sum(if(WHITE_LIST=0,1,0))=0,-999,cast(sum (if(CREDIT_CODE='0' and WHITE_LIST=0,1,0)) as float )/sum(if(WHITE_LIST=0,1,0))) as float )                                  as PR_UWL_CCT_1H,       -- 1小时时间窗内的贷款授信申请通过且未命中白名单客户的笔数/申请且未命中白名单客户的笔数
    sum (if(BLACK_LIST=0,1,0))                                                                                                                                      as AF_ACC_C_CT_1H,      -- 1小时时间窗内的贷款授信申请反欺诈通过笔数
    -- cast (sum (if(BLACK_LIST='0',1,0))/count (SESSION_ID) as float)                                                                                              as AF_PR_CT_1H,         -- 1小时时间窗内的贷款授信申请反欺诈通过笔数/申请笔数
    cast ( if (sum (if(CREDIT_CODE='0',1,0))=0,-999,sum(if(CREDIT_CODE='0',CREDIT_SCORE_1,0))/sum (if(CREDIT_CODE='0',1,0))) as double )                            as AVG_ACC_CCSCT_1H,    -- 1小时时间窗内的贷款授信申请通过客户的客户卡评分均值           自定义函数
    cast ( if (sum (if(CREDIT_CODE='0',1,0))=0,-999,sum(if(CREDIT_CODE='0',APPLY_SCORE,0))/sum (if(CREDIT_CODE='0',1,0))) as double )                               as AVG_ACC_PCSCT_1H,    -- 1小时时间窗内的贷款授信申请通过客户的产品卡评分均值
    cast ( if(customcount(CUST_ID,CREDIT_CODE)=0,-999,customcount(CUST_ID,CREDIT_CODE,if(CREDIT_SCORE_1 is null,0,CAST(CREDIT_SCORE_1 AS INT)),if(CREDIT_LIMIT is null,0.0,CREDIT_LIMIT))/customcount(CUST_ID,CREDIT_CODE)) as double)     as RATE_OC_1H           -- 1小时时间窗内的贷款授信申请通过客户的客户卡评分低于220分且额度高于4万元的客户数量/申请通过客户数量 自定义函数
from kafka_result_info_200 group by HOP(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR),BUSINESS_TYPE_CD;

create view mid_1h as
select
mid_1.BUSINESS_TYPE_CD,
BUSINESS_TYPE_NAME,
    apply_view1.dt,
    APP_C_CT_1H,
    ACC_C_CT_1H,
    if(APP_C_CT_1H=0,-999,cast (cast(ACC_C_CT_1H as float)/APP_C_CT_1H as float)) as PR_CT_1H,
    ACC_C_CCT_1H,
    if(APP_C_CT_1H=0,-999,cast (cast(ACC_C_CCT_1H as double )/APP_C_CT_1H as double )) as AVG_AC_CCT_1H,
    APP_C_WL_CCT_1H,
    ACC_C_WL_CCT_1H,
    PR_WL_CCT_1H,
    APP_C_UWL_CCT_1H,
    ACC_C_UWL_CCT_1H,
    PR_UWL_CCT_1H,
    AF_ACC_C_CT_1H,
    if(APP_C_CT_1H=0,-999,cast(cast(AF_ACC_C_CT_1H as float )/APP_C_CT_1H as float )) as AF_PR_CT_1H,
    AVG_ACC_CCSCT_1H,
    AVG_ACC_PCSCT_1H,
    RATE_OC_1H
from mid_1,apply_view1 where mid_1.BUSINESS_TYPE_CD=apply_view1.BUSINESS_TYPE_CD and mid_1.dt between apply_view1.dt - interval '2' second and apply_view1.dt + interval '2' second;



    -- 2个小时窗口
create view mid_2 as
select
BUSINESS_TYPE_CD,
    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '2' HOUR)                                                                                                          as dt,
    sum (if(CREDIT_CODE='0',1,0))                                                                                                                                   as ACC_C_CT_2H,         -- 2小时时间窗内的贷款授信申请通过笔数
    -- cast (sum (if(CREDIT_CODE='0',1,0))/count (SESSION_ID) as float)                                                                                             as PR_CT_2H,            -- 2小时时间窗内的贷款授信申请通过笔数/申请笔数
    customcount(CUST_ID,CREDIT_CODE)                                                                                                                                as ACC_C_CCT_2H,        -- 2小时时间窗内的贷款授信申请通过客户数                   自定义函数求 集合的个数
    -- cast (customcount(CUST_ID,CREDIT_CODE)/count (SESSION_ID) as double)                                                                                         as AVG_AC_CCT_2H,       -- 2小时时间窗内的贷款授信申请通过客户数/申请笔数
    sum (if(WHITE_LIST=1,1,0))                                                                                                                                      as APP_C_WL_CCT_2H,     -- 2小时时间窗内的贷款授信申请且命中白名单客户的笔数
    sum (if(CREDIT_CODE='0' and WHITE_LIST=1,1,0))                                                                                                                  as ACC_C_WL_CCT_2H,     -- 2小时时间窗内的贷款授信申请通过且命中白名单客户的笔数
    cast (if (sum(if(WHITE_LIST=1,1,0))=0,-999,cast(sum (if(CREDIT_CODE='0' and WHITE_LIST=1,1,0)) as float )/sum (if(WHITE_LIST=1,1,0))) as float)                                 as PR_WL_CCT_2H,        -- 2小时时间窗内的贷款授信申请通过且命中白名单客户的笔数/申请且命中白名单客户的笔数
    sum (if(WHITE_LIST=0,1,0))                                                                                                                                      as APP_C_UWL_CCT_2H,    -- 2小时时间窗内的贷款授信申请且未命中白名单客户的笔数
    sum (if(CREDIT_CODE='0' and WHITE_LIST=0,1,0))                                                                                                                  as ACC_C_UWL_CCT_2H,    -- 2小时时间窗内的贷款授信申请通过且未命中白名单客户的笔数
    cast (if(sum(if(WHITE_LIST=0,1,0))=0,-999,cast(sum (if(CREDIT_CODE='0' and WHITE_LIST=0,1,0)) as float )/sum(if(WHITE_LIST=0,1,0))) as float )                                  as PR_UWL_CCT_2H,       -- 2小时时间窗内的贷款授信申请通过且未命中白名单客户的笔数/申请且未命中白名单客户的笔数
    sum (if(BLACK_LIST=0,1,0))                                                                                                                                      as AF_ACC_C_CT_2H,      -- 2小时时间窗内的贷款授信申请反欺诈通过笔数
    -- cast (sum (if(BLACK_LIST='0',1,0))/count (SESSION_ID) as float)                                                                                              as AF_PR_CT_2H,         -- 2小时时间窗内的贷款授信申请反欺诈通过笔数/申请笔数
    cast ( if (sum (if(CREDIT_CODE='0',1,0))=0,-999,sum(if(CREDIT_CODE='0',CREDIT_SCORE_1,0))/sum (if(CREDIT_CODE='0',1,0))) as double )                            as AVG_ACC_CCSCT_2H,    -- 2小时时间窗内的贷款授信申请通过客户的客户卡评分均值           自定义函数
    cast ( if (sum (if(CREDIT_CODE='0',1,0))=0,-999,sum(if(CREDIT_CODE='0',APPLY_SCORE,0))/sum (if(CREDIT_CODE='0',1,0))) as double )                               as AVG_ACC_PCSCT_2H,    -- 2小时时间窗内的贷款授信申请通过客户的产品卡评分均值
    cast ( if(customcount(CUST_ID,CREDIT_CODE)=0,-999,customcount(CUST_ID,CREDIT_CODE,if(CREDIT_SCORE_1 is null,0,CAST(CREDIT_SCORE_1 AS INT)),if(CREDIT_LIMIT is null,0.0,CREDIT_LIMIT))/customcount(CUST_ID,CREDIT_CODE)) as double)     as RATE_OC_2H           -- 2小时时间窗内的贷款授信申请通过客户的客户卡评分低于220分且额度高于4万元的客户数量/申请通过客户数量 自定义函数
from kafka_result_info_200 group by HOP(et,INTERVAL '10' SECOND,INTERVAL '2' HOUR),BUSINESS_TYPE_CD;

create view mid_2h as
select
mid_2.BUSINESS_TYPE_CD,
    apply_view2.dt,
    APP_C_CT_2H,
    ACC_C_CT_2H,
    if(APP_C_CT_2H=0,-999,cast (cast(ACC_C_CT_2H as float )/APP_C_CT_2H as float)) as PR_CT_2H,
    ACC_C_CCT_2H,
    if(APP_C_CT_2H=0,-999,cast (cast(ACC_C_CCT_2H as double )/APP_C_CT_2H as double )) as AVG_AC_CCT_2H,
    APP_C_WL_CCT_2H,
    ACC_C_WL_CCT_2H,
    PR_WL_CCT_2H,
    APP_C_UWL_CCT_2H,
    ACC_C_UWL_CCT_2H,
    PR_UWL_CCT_2H,
    AF_ACC_C_CT_2H,
    if(APP_C_CT_2H=0,-999,cast(cast(AF_ACC_C_CT_2H as float )/APP_C_CT_2H as float )) as AF_PR_CT_2H,
    AVG_ACC_CCSCT_2H,
    AVG_ACC_PCSCT_2H,
    RATE_OC_2H
from mid_2,apply_view2 where mid_2.BUSINESS_TYPE_CD=apply_view2.BUSINESS_TYPE_CD and mid_2.dt between apply_view2.dt - interval '2' second and apply_view2.dt + interval '2' second;




    -- 6个小时窗口
create view mid_6 as
select
BUSINESS_TYPE_CD,
    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '6' HOUR)                                                                                                          as dt,
    sum (if(CREDIT_CODE='0',1,0))                                                                                                                                   as ACC_C_CT_6H,         -- 6小时时间窗内的贷款授信申请通过笔数
    -- cast (sum (if(CREDIT_CODE='0',1,0))/count (SESSION_ID) as float)                                                                                             as PR_CT_6H,            -- 6小时时间窗内的贷款授信申请通过笔数/申请笔数
    customcount(CUST_ID,CREDIT_CODE)                                                                                                                                as ACC_C_CCT_6H,        -- 6小时时间窗内的贷款授信申请通过客户数                   自定义函数求 集合的个数
    -- cast (customcount(CUST_ID,CREDIT_CODE)/count (SESSION_ID) as double)                                                                                         as AVG_AC_CCT_6H,       -- 6小时时间窗内的贷款授信申请通过客户数/申请笔数
    sum (if(WHITE_LIST=1,1,0))                                                                                                                                      as APP_C_WL_CCT_6H,     -- 6小时时间窗内的贷款授信申请且命中白名单客户的笔数
    sum (if(CREDIT_CODE='0' and WHITE_LIST=1,1,0))                                                                                                                  as ACC_C_WL_CCT_6H,     -- 6小时时间窗内的贷款授信申请通过且命中白名单客户的笔数
    cast (if (sum(if(WHITE_LIST=1,1,0))=0,-999,cast (sum (if(CREDIT_CODE='0' and WHITE_LIST=1,1,0)) as float )/sum (if(WHITE_LIST=1,1,0))) as float)                                 as PR_WL_CCT_6H,        -- 6小时时间窗内的贷款授信申请通过且命中白名单客户的笔数/申请且命中白名单客户的笔数
    sum (if(WHITE_LIST=0,1,0))                                                                                                                                      as APP_C_UWL_CCT_6H,    -- 6小时时间窗内的贷款授信申请且未命中白名单客户的笔数
    sum (if(CREDIT_CODE='0' and WHITE_LIST=0,1,0))                                                                                                                  as ACC_C_UWL_CCT_6H,    -- 6小时时间窗内的贷款授信申请通过且未命中白名单客户的笔数
    cast (if(sum(if(WHITE_LIST=0,1,0))=0,-999,cast (sum (if(CREDIT_CODE='0' and WHITE_LIST=0,1,0)) as float )/sum(if(WHITE_LIST=0,1,0))) as float )                                  as PR_UWL_CCT_6H,       -- 6小时时间窗内的贷款授信申请通过且未命中白名单客户的笔数/申请且未命中白名单客户的笔数
    sum (if(BLACK_LIST=0,1,0))                                                                                                                                      as AF_ACC_C_CT_6H,      -- 6小时时间窗内的贷款授信申请反欺诈通过笔数
    -- cast (sum (if(BLACK_LIST='0',1,0))/count (SESSION_ID) as float)                                                                                              as AF_PR_CT_6H,         -- 6小时时间窗内的贷款授信申请反欺诈通过笔数/申请笔数
    cast ( if (sum (if(CREDIT_CODE='0',1,0))=0,-999,sum(if(CREDIT_CODE='0',CREDIT_SCORE_1,0))/sum (if(CREDIT_CODE='0',1,0))) as double )                            as AVG_ACC_CCSCT_6H,    -- 6小时时间窗内的贷款授信申请通过客户的客户卡评分均值           自定义函数
    cast ( if (sum (if(CREDIT_CODE='0',1,0))=0,-999,sum(if(CREDIT_CODE='0',APPLY_SCORE,0))/sum (if(CREDIT_CODE='0',1,0))) as double )                               as AVG_ACC_PCSCT_6H,    -- 6小时时间窗内的贷款授信申请通过客户的产品卡评分均值
    cast ( if(customcount(CUST_ID,CREDIT_CODE)=0,-999,customcount(CUST_ID,CREDIT_CODE,if(CREDIT_SCORE_1 is null,0,CAST(CREDIT_SCORE_1 AS INT)),if(CREDIT_LIMIT is null,0.0,CREDIT_LIMIT))/customcount(CUST_ID,CREDIT_CODE)) as double)     as RATE_OC_6H           -- 6小时时间窗内的贷款授信申请通过客户的客户卡评分低于220分且额度高于4万元的客户数量/申请通过客户数量 自定义函数
from kafka_result_info_200 group by HOP(et,INTERVAL '10' SECOND,INTERVAL '6' HOUR),BUSINESS_TYPE_CD;

create view mid_6h as
select
mid_6.BUSINESS_TYPE_CD,
    apply_view6.dt,
    APP_C_CT_6H,
    ACC_C_CT_6H,
    if(APP_C_CT_6H=0,-999,cast (cast(ACC_C_CT_6H as float )/APP_C_CT_6H as float)) as PR_CT_6H,
    ACC_C_CCT_6H,
    if(APP_C_CT_6H=0,-999,cast (cast (ACC_C_CCT_6H as double )/APP_C_CT_6H as double )) as AVG_AC_CCT_6H,
    APP_C_WL_CCT_6H,
    ACC_C_WL_CCT_6H,
    PR_WL_CCT_6H,
    APP_C_UWL_CCT_6H,
    ACC_C_UWL_CCT_6H,
    PR_UWL_CCT_6H,
    AF_ACC_C_CT_6H,
    if(APP_C_CT_6H=0,-999,cast(cast(AF_ACC_C_CT_6H as float )/APP_C_CT_6H as float )) as AF_PR_CT_6H,
    AVG_ACC_CCSCT_6H,
    AVG_ACC_PCSCT_6H,
    RATE_OC_6H
from mid_6,apply_view6 where mid_6.BUSINESS_TYPE_CD=apply_view6.BUSINESS_TYPE_CD and mid_6.dt between apply_view6.dt - interval '2' second and apply_view6.dt + interval '2' second;

-- 把三个mid view整合在一起
create view mid_table1 as
select
BUSINESS_TYPE_NAME,
APP_C_CT_1H,
ACC_C_CT_1H,
PR_CT_1H,
ACC_C_CCT_1H,
AVG_AC_CCT_1H,
APP_C_WL_CCT_1H,
ACC_C_WL_CCT_1H,
PR_WL_CCT_1H,
APP_C_UWL_CCT_1H,
ACC_C_UWL_CCT_1H,
PR_UWL_CCT_1H,
AF_ACC_C_CT_1H,
AF_PR_CT_1H,
AVG_ACC_CCSCT_1H,
AVG_ACC_PCSCT_1H,
RATE_OC_1H,
mid_2h.*
from mid_1h,mid_2h where mid_1h.BUSINESS_TYPE_CD=mid_2h.BUSINESS_TYPE_CD and mid_1h.dt between mid_2h.dt - interval '2' second and mid_2h.dt + interval '2' second;

-- 此时表中有多余的两个字段 dt0 dt1
create view mid_table2 as
select
APP_C_CT_6H,
ACC_C_CT_6H,
PR_CT_6H,
ACC_C_CCT_6H,
AVG_AC_CCT_6H,
APP_C_WL_CCT_6H,
ACC_C_WL_CCT_6H,
PR_WL_CCT_6H,
APP_C_UWL_CCT_6H,
ACC_C_UWL_CCT_6H,
PR_UWL_CCT_6H,
AF_ACC_C_CT_6H,
AF_PR_CT_6H,
AVG_ACC_CCSCT_6H,
AVG_ACC_PCSCT_6H,
RATE_OC_6H,
mid_table1.*
from mid_table1,mid_6h where mid_table1.BUSINESS_TYPE_CD=mid_6h.BUSINESS_TYPE_CD and mid_table1.dt between mid_6h.dt - interval '2' second and mid_6h.dt + interval '2' second;


insert into kafka_product_monitor_suixindai
select
    mid_table2.BUSINESS_TYPE_CD,
    BUSINESS_TYPE_NAME,
    mid_table2.dt,
    APP_C_CT_1H,
    APP_C_CT_2H,
    APP_C_CT_6H,
    ACC_C_CT_1H,
    ACC_C_CT_2H,
    ACC_C_CT_6H,
    PR_CT_1H,
    PR_CT_2H,
    PR_CT_6H,
    ACC_C_CCT_1H,
    ACC_C_CCT_2H,
    ACC_C_CCT_6H,
    AVG_AC_CCT_1H,
    AVG_AC_CCT_2H,
    AVG_AC_CCT_6H,
    APP_C_WL_CCT_1H,
    APP_C_WL_CCT_2H,
    APP_C_WL_CCT_6H,
    ACC_C_WL_CCT_1H,
    ACC_C_WL_CCT_2H,
    ACC_C_WL_CCT_6H,
    PR_WL_CCT_1H,
    PR_WL_CCT_2H,
    PR_WL_CCT_6H,
    APP_C_UWL_CCT_1H,
    APP_C_UWL_CCT_2H,
    APP_C_UWL_CCT_6H,
    ACC_C_UWL_CCT_1H,
    ACC_C_UWL_CCT_2H,
    ACC_C_UWL_CCT_6H,
    PR_UWL_CCT_1H,
    PR_UWL_CCT_2H,
    PR_UWL_CCT_6H,
    AF_ACC_C_CT_1H,
    AF_ACC_C_CT_2H,
    AF_ACC_C_CT_6H,
    AF_PR_CT_1H,
    AF_PR_CT_2H,
    AF_PR_CT_6H,
    AVG_ACC_CCSCT_1H,
    AVG_ACC_CCSCT_2H,
    AVG_ACC_CCSCT_6H,
    AVG_ACC_PCSCT_1H,
    AVG_ACC_PCSCT_2H,
    AVG_ACC_PCSCT_6H,
    RATE_OC_1H,
    RATE_OC_2H,
    RATE_OC_6H
from mid_table2 where mid_table2.BUSINESS_TYPE_CD='P0036';











