--kafka授信申请入参表
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
      'connector.properties.group.id'='dev_flink1',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
      'format.type' = 'json',
      'update-mode' = 'append'
      );

--kafka授信结果入参表
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
     CREDIT_SCORE_1  DOUBLE,
     CREDIT_SCORE_2 DOUBLE,
     CREDIT_SCORE_3  DOUBLE,
     ANTI_FRAUD_SCORE_1  DOUBLE,
     ANTI_FRAUD_SCORE_2  DOUBLE,
     CREDIT_TIME  BIGINT,
    et AS TO_TIMESTAMP(FROM_UNIXTIME(CREDIT_TIME/1000)),
    WATERMARK FOR et AS et - INTERVAL '5' SECOND
    )WITH (
      'connector.type' = 'kafka',
    'connector.version' = 'universal',
      'connector.topic' = 'kafkaCreditResultInfo_1234',
      'connector.properties.group.id'='dev_flink1',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
      'format.type' = 'json',
      'update-mode' = 'append'
    );

--kafka sink表
    CREATE TABLE kafka_product_monitor_200 (
      P_ID STRING,
      DT TIMESTAMP(3),
      CH INT,
      APP_C_PH BIGINT NOT NULL,
      R_C_PH BIGINT NOT NULL,
      ACC_C_PH BIGINT NOT NULL,
      APP_P_H FLOAT,
      APP_P_D FLOAT,
      PR FLOAT NOT NULL,
      PR_P_H FLOAT,
      PR_P_D FLOAT,
      AVG_CS_1 DOUBLE,
      AVG_CS_2 DOUBLE,
      AVG_CS_3 DOUBLE,
      AVG_AFS_1 DOUBLE,
      AVG_AFS_2 DOUBLE,
      AVG_LIMIT DOUBLE,
      AVG_RATE FLOAT
      )
       WITH (
      'connector.type' = 'kafka',
      'connector.version' = 'universal',
      'connector.topic' = 'weipinhua_monitor',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
      'format.type' = 'json',
      'update-mode' = 'append'
    );
  
  




--apply_info表滑窗口1小时 10秒输出一次
CREATE VIEW kafka_apply_info_hop_views AS
SELECT
BUSINESS_TYPE_CD,
HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR) AS dt,
HOUR(HOP_END(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR)) AS cur_hour,
COUNT(*) as apply_num
FROM kafka_apply_info_200
GROUP BY HOP(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR),BUSINESS_TYPE_CD
;

--result_info表滑动窗口1小时  10秒输出一次
CREATE VIEW kafka_result_info_hop_views AS
SELECT
BUSINESS_TYPE_CD,
HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR) AS dt,
HOUR(HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR)) AS cur_hour,
COUNT(*) AS return_num,
cast(SUM(CASE WHEN CREDIT_CODE='0' then 1 ELSE 0 END) as bigint) AS success_num,
AVG(CREDIT_SCORE_1) as AVG_CS_1,
AVG(CREDIT_SCORE_2) as AVG_CS_2,
AVG(CREDIT_SCORE_3) as AVG_CS_3,
AVG(ANTI_FRAUD_SCORE_1) as AVG_AFS_1,
AVG(ANTI_FRAUD_SCORE_2) as AVG_AFS_2,
AVG(INTEREST_RATE) as AVG_RATE,
AVG(CREDIT_LIMIT) as AVG_LIMIT
FROM kafka_result_info_200
GROUP BY HOP(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR),BUSINESS_TYPE_CD
;

--取上一个窗口的apply_num    取前一天的apply_num
CREATE VIEW kafka_apply_info_previous_views AS
SELECT
dt,
cur_hour,
apply_num,
BUSINESS_TYPE_CD,
previous_lag_long(apply_num,1,cast(0 as bigint)) OVER w AS last_num,
previous_lag_long(apply_num,8640,cast(0 as bigint)) OVER w AS last_24_num,
DATE_FORMAT(dt, 'yyyy-MM-dd HH:mm:ss') AS data_time
FROM kafka_apply_info_hop_views
WINDOW w AS (PARTITION BY BUSINESS_TYPE_CD ORDER BY dt ROWS BETWEEN UNBOUNDED
PRECEDING AND CURRENT ROW);


--计算当前窗口与历史窗口的比率

CREATE VIEW kafka_apply_info_previous_day_views AS
SELECT 
dt,
apply_num,BUSINESS_TYPE_CD,
cur_hour,
data_time,
last_num,
last_24_num,
if(last_num=0,-999,CAST(apply_num AS float)/CAST(last_num AS float)) AS apply_compared_with_previous_hour,
if(last_24_num=0,-999,CAST(apply_num AS float)/CAST(last_24_num AS float)) AS apply_compared_with_previous_day
FROM kafka_apply_info_previous_views
;


--计算结果表上一个窗口/前一天的数量
CREATE VIEW kafka_result_info_previous_views AS
SELECT
BUSINESS_TYPE_CD,
dt,
cur_hour,
return_num,
success_num,
AVG_CS_1,
AVG_CS_2,
AVG_CS_3,
AVG_AFS_1,
AVG_AFS_2,
AVG_RATE,
AVG_LIMIT,
previous_lag_long(success_num,1,cast(0 as bigint)) OVER w AS last_num,
previous_lag_long(success_num,8640,cast(0 as bigint)) OVER w AS last_24_num,
DATE_FORMAT(dt, 'yyyy-MM-dd HH:mm:ss') AS data_time
FROM kafka_result_info_hop_views
WINDOW w AS (PARTITION BY BUSINESS_TYPE_CD ORDER BY dt ROWS BETWEEN UNBOUNDED
PRECEDING AND CURRENT ROW);



--窗口结果
INSERT INTO kafka_product_monitor_200
SELECT
apply.BUSINESS_TYPE_CD AS P_ID,
CAST(apply.data_time AS TIMESTAMP(3)) AS DT,
CAST(apply.cur_hour AS INT) AS CH,
apply.apply_num AS APP_C_PH,
results.return_num AS R_C_PH,
results.success_num AS ACC_C_PH,
apply.apply_compared_with_previous_hour as APP_P_H,
apply.apply_compared_with_previous_day as APP_P_D,
if(apply.apply_num=0,-999,CAST(results.success_num AS float)/CAST(apply.apply_num AS float)) AS PR,
if(apply.apply_num=0 or results.last_num<=0 or apply.last_num<=0,-999,(CAST(results.success_num AS float)/CAST(apply.apply_num AS float))/(CAST(results.last_num AS FLOAT)/CAST(apply.last_num AS FLOAT))) as PR_P_H,
if(apply.apply_num=0 or results.last_24_num<=0 or apply.last_24_num<=0,-999,(CAST(results.success_num AS float)/CAST(apply.apply_num AS float))/(CAST(results.last_24_num AS FLOAT)/CAST(apply.last_24_num AS FLOAT))) as PR_P_D,
results.AVG_CS_1 AS AVG_CS_1,
results.AVG_CS_2 AS AVG_CS_2,
results.AVG_CS_3 AS AVG_CS_3,
results.AVG_AFS_1 AS AVG_AFS_1,
results.AVG_AFS_2 AS AVG_AFS_2,
results.AVG_LIMIT AS AVG_LIMIT, --授信额度平均值
CAST(results.AVG_RATE AS FLOAT) AS AVG_RATE --授信利率平均值
FROM kafka_apply_info_previous_day_views apply , kafka_result_info_previous_views results
WHERE apply.BUSINESS_TYPE_CD = results.BUSINESS_TYPE_CD AND
apply.dt BETWEEN results.dt - INTERVAL '2' SECOND AND
results.dt + INTERVAL '2' SECOND 
;
