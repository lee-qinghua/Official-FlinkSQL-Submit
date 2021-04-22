-- 目前按照所有字段都不为null的情况计算的，看接口字段这些字段都是必须的。
-- 如果可以为null，创建view的时候，if(xxx is null,0,count(1) over w),在最后结果join的时候可以先处理source,if(xxx is null,'',xxx) 设置一个空值，这样可以join上
-- 但是还有一个问题，连续两条一样的xxx 数据，APPLY_TIME也相同，这样会join出两条结果，然后再对join的结果去重？
    CREATE TABLE mayi_info_1 (
      SESSION_ID                 STRING,
      APP_NO                     STRING,
      BUSINESS_TYPE_CD           STRING,
      BUSINESS_TYPE_NAME         STRING,
      APPLY_TIME                 BIGINT,
      STEP_TYPE                  STRING,    --流程类型
      NODE_TYPE                  STRING,    --节点类型
      CUST_SOURCE                STRING,    --来源系统
      CHANNEL_SOURCE             STRING,    --来源渠道
      CUST_ID                    STRING,    --客户编号
      CUST_NAME                  STRING,    --客户名称
      GENDER_CD                  STRING,    --性别代码
      BIRTH_DT                   STRING,    --出生日期
      CERT_TYPE                  STRING,    --证件类型代码
      CERT_NO                    STRING,    --证件号码
      INDE_VALID_DT              STRING,    --证件有效期
      CUSTOM_EVALU_LEVEL         STRING,    --我行vip用户
      NATIONALITY_CD             STRING,    --国籍代码
      APPL_CHAN                  STRING,    --守信申请渠道
      CERT_NO_2                  STRING,    --证件号码2
      CERT_TYPE_2                STRING,    --证件类型2
      CERT_VALID_END_DT          STRING,    --证件有效期
      NAME                       STRING,    --申请人姓名
      MOBILE_NO                  STRING,    --申请手机号
      PRODUCT_CODE               STRING,    --产品标识
      BIZ_MODE                   STRING,    --业务模式
      LOAN_MODE                  STRING,    --出资模式
      APPLY_TYPE                 STRING,    --申请类型
      SPOUSE_NAME                STRING,    --配偶姓名
      SPOUSE_CERT_NO             STRING,    --配偶身份证
      SPOUSE_MOBILE_NO           STRING,    --配偶电话
      COMP_NAME                  STRING,    --工作单位
      CUR_MARRIAGE_STATUS_CD     STRING,    --婚姻状态
      ADDR_DESC                  STRING,    --地址
      ADDR_USAGE_EDC             STRING,    --地址用途
      MOBILE_NUM                 STRING,    --手机号码
      DUTY_CD                    STRING,    -- 职务代码
      TITLE_CD                   STRING,    --职称代码
      OCCUPATION_CD              STRING,    --职业代码
      EDUCATION_LEVEL_CD         STRING,    --学历代码
      CERT_ADDR                  STRING,    --身份证地址
      STANDARD_ADDR              STRING,    --标准化地址
      et AS TO_TIMESTAMP(FROM_UNIXTIME(APPLY_TIME)),
      WATERMARK FOR et AS et
    )
    WITH (
      'connector.type' = 'kafka',
      'connector.version' = 'universal',
      'connector.topic' = 'mayi_info3',
      'connector.properties.group.id'='dev_flink1',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
      'format.type' = 'json',
      'update-mode' = 'append',
      'connector.startup-mode' = 'latest-offset'
);


CREATE TABLE sink_table(
    cert_no STRING,
    one_days_cert_apply_num bigint,
    one_days_cert_contact_num bigint,
    one_days_mobile_contact_num bigint,
    one_days_cert_or_mobile_num bigint,
    one_days_cert_couple_num bigint,
    one_days_cert_couple_mobile_num bigint,
    one_days_company_apply_num bigint,
    one_days_address_apply_num bigint,
    three_days_cert_apply_num bigint,
    three_days_cert_contact_num bigint,
    three_days_mobile_contact_num bigint,
    three_days_cert_or_mobile_num bigint,
    three_days_cert_couple_num bigint,
    three_days_cert_couple_mobile_num bigint,
    three_days_company_apply_num bigint,
    three_days_address_apply_num bigint,
    seven_days_cert_apply_num bigint,
    seven_days_cert_contact_num bigint,
    seven_days_mobile_contact_num bigint,
    seven_days_cert_or_mobile_num bigint,
    seven_days_cert_couple_num bigint,
    seven_days_cert_couple_mobile_num bigint,
    seven_days_company_apply_num bigint,
    seven_days_address_apply_num bigint
)with(
      'connector.type' = 'kafka',
      'connector.version' = 'universal',
      'connector.topic' = 'mayi_sink',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
      'format.type' = 'json',
      'update-mode' = 'append'
);
-- ===============================================================7day start================================================================================
-- 7身份证关联申请信息个数
create view a as
select
    et,
    CERT_NO,
    APPLY_TIME,
    count(1) OVER w as cnt
from mayi_info_1
    window w as (partition by CERT_NO ORDER BY et RANGE BETWEEN interval '7' DAY preceding and current row );

--7天内手机号申请次数
create view d as
select
    et,
    APPLY_TIME,
    MOBILE_NUM,
    count (1) OVER w as cnt
from mayi_info_1
    window w as (partition by MOBILE_NUM ORDER BY et RANGE BETWEEN interval '7' DAY preceding and current row );
--7天内手机号&身份证号申请次数
create view ad as
select
    et,
    APPLY_TIME,
    CERT_NO,
    MOBILE_NUM,
    count (1) OVER w as cnt
from mayi_info_1
    window w as (partition by MOBILE_NUM,CERT_NO ORDER BY et RANGE BETWEEN interval '7' DAY preceding and current row );


--7天内同一单位名称申请贷款人数
create view e as
select
    et,
    APPLY_TIME,
    COMP_NAME,
    count (1) OVER w as cnt
from mayi_info_1
    window w as (partition by COMP_NAME ORDER BY et RANGE BETWEEN interval '7' DAY preceding and current row );

-- 7天内同一地址申请次数
create view f as
select
    et,
    APPLY_TIME,
    STANDARD_ADDR,
    count (1) OVER w as cnt
from mayi_info_1
    window w as (partition by STANDARD_ADDR ORDER BY et RANGE BETWEEN interval '7' DAY preceding and current row );



-- 选择CERT_NO 本人身份证的view
create view fake_data as
select
    et,
    APPLY_TIME,
    APP_NO,
    CERT_NO,
    'fake' as flag
from mayi_info_1;
-- 选择SPOUSE_CERT_NO 联系人身份证的view
create view real_data as
select
    et,
    APPLY_TIME,
    APP_NO,
    SPOUSE_CERT_NO,
    'real' as flag
from mayi_info_1;

-- 两个view union all 相当于把两个view中的数据合在一起
create view union_view as
select *
from
    (select et,APPLY_TIME,APP_NO,CERT_NO as id,flag from fake_data)
union all
     (select et,APPLY_TIME,APP_NO,SPOUSE_CERT_NO as id,flag from real_data);

-- 根据union_view 计算次数的时候，注意flag  sum(if(flag='fake',0,1))
create view union_data as
select
    id,
    et,
    APPLY_TIME,
    sum(if(flag='fake',0,1)) over(partition by id order by et range between interval '7' DAY preceding and current row) as cnt
from union_view;

-- phone number
create view fake_data_phone as
select
    et,
    APPLY_TIME,
    APP_NO,
    MOBILE_NUM,
    'fake' as flag
from mayi_info_1;

create view real_data_phone as
select
    et,
    APPLY_TIME,
    APP_NO,
    SPOUSE_MOBILE_NO,
    'real' as flag
from mayi_info_1;

create view union_view_phone as
select *
from
    (select et,APPLY_TIME,APP_NO,MOBILE_NUM as phone,flag from fake_data_phone)
union all
     (select et,APPLY_TIME,APP_NO,SPOUSE_MOBILE_NO as phone,flag from real_data_phone);

create view union_data_phone as
select
    phone,
    et,
    APPLY_TIME,
    sum(if(flag='fake',0,1)) over(partition by phone order by et range between interval '7' DAY preceding and current row) as cnt
from union_view_phone;


create view g as
select
    CERT_NO,
    APPLY_TIME,
    et,
    count(distinct SPOUSE_CERT_NO) over(partition by CERT_NO order by et range between interval '7' DAY preceding and current row) as cnt
from mayi_info_1;

create view h as
select
    CERT_NO,
    APPLY_TIME,
    et,
    count(distinct SPOUSE_MOBILE_NO) over(partition by CERT_NO order by et range between interval '7' DAY preceding and current row) as cnt
from mayi_info_1;

-- ===============================================================7day end  ================================================================================
-- ===============================================================3day start================================================================================
-- 3天身份证关联申请信息个数
create view a_3 as
select
    et,
    CERT_NO,
    APPLY_TIME,
    count(1) OVER w as cnt
from mayi_info_1
    window w as (partition by CERT_NO ORDER BY et RANGE BETWEEN interval '3' DAY preceding and current row );

--3天内手机号申请次数
create view d_3 as
select
    et,
    APPLY_TIME,
    MOBILE_NUM,
    count (1) OVER w as cnt
from mayi_info_1
    window w as (partition by MOBILE_NUM ORDER BY et RANGE BETWEEN interval '3' DAY preceding and current row );
--3天内手机号&身份证号申请次数
create view ad_3 as
select
    et,
    APPLY_TIME,
    CERT_NO,
    MOBILE_NUM,
    count (1) OVER w as cnt
from mayi_info_1
    window w as (partition by MOBILE_NUM,CERT_NO ORDER BY et RANGE BETWEEN interval '3' DAY preceding and current row );


--3天内同一单位名称申请贷款人数
create view e_3 as
select
    et,
    APPLY_TIME,
    COMP_NAME,
    count (1) OVER w as cnt
from mayi_info_1
    window w as (partition by COMP_NAME ORDER BY et RANGE BETWEEN interval '3' DAY preceding and current row );

-- 3天内同一地址申请次数
create view f_3 as
select
    et,
    APPLY_TIME,
    STANDARD_ADDR,
    count (1) OVER w as cnt
from mayi_info_1
    window w as (partition by STANDARD_ADDR ORDER BY et RANGE BETWEEN interval '3' DAY preceding and current row );



-- 选择CERT_NO 本人身份证的view
create view fake_data_3 as
select
    et,
    APPLY_TIME,
    APP_NO,
    CERT_NO,
    'fake' as flag
from mayi_info_1;
-- 选择SPOUSE_CERT_NO 联系人身份证的view
create view real_data_3 as
select
    et,
    APPLY_TIME,
    APP_NO,
    SPOUSE_CERT_NO,
    'real' as flag
from mayi_info_1;

-- 两个view union all 相当于把两个view中的数据合在一起
create view union_view_3 as
select *
from
    (select et,APPLY_TIME,APP_NO,CERT_NO as id,flag from fake_data_3)
union all
     (select et,APPLY_TIME,APP_NO,SPOUSE_CERT_NO as id,flag from real_data_3);

-- 根据union_view 计算次数的时候，注意flag  sum(if(flag='fake',0,1))
create view union_data_3 as
select
    id,
    et,
    APPLY_TIME,
    sum(if(flag='fake',0,1)) over(partition by id order by et range between interval '3' DAY preceding and current row) as cnt
from union_view_3;

-- phone number
create view fake_data_phone_3 as
select
    et,
    APPLY_TIME,
    APP_NO,
    MOBILE_NUM,
    'fake' as flag
from mayi_info_1;

create view real_data_phone_3 as
select
    et,
    APPLY_TIME,
    APP_NO,
    SPOUSE_MOBILE_NO,
    'real' as flag
from mayi_info_1;

create view union_view_phone_3 as
select *
from
    (select et,APPLY_TIME,APP_NO,MOBILE_NUM as phone,flag from fake_data_phone_3)
union all
     (select et,APPLY_TIME,APP_NO,SPOUSE_MOBILE_NO as phone,flag from real_data_phone_3);

create view union_data_phone_3 as
select
    phone,
    et,
    APPLY_TIME,
    sum(if(flag='fake',0,1)) over(partition by phone order by et range between interval '3' DAY preceding and current row) as cnt
from union_view_phone_3;

create view g_3 as
select
    CERT_NO,
    APPLY_TIME,
    et,
    count(distinct SPOUSE_CERT_NO) over(partition by CERT_NO order by et range between interval '3' DAY preceding and current row) as cnt
from mayi_info_1;

create view h_3 as
select
    CERT_NO,
    APPLY_TIME,
    et,
    count(distinct SPOUSE_MOBILE_NO) over(partition by CERT_NO order by et range between interval '3' DAY preceding and current row) as cnt
from mayi_info_1;
-- ===============================================================3day end================================================================================




-- ===============================================================1day start================================================================================
-- 1天身份证关联申请信息个数
create view a_1 as
select
    et,
    CERT_NO,
    APPLY_TIME,
    count(1) OVER w as cnt
from mayi_info_1
    window w as (partition by CERT_NO ORDER BY et RANGE BETWEEN interval '1' DAY preceding and current row );

--1天内手机号申请次数
create view d_1 as
select
    et,
    APPLY_TIME,
    MOBILE_NUM,
    count (1) OVER w as cnt
from mayi_info_1
    window w as (partition by MOBILE_NUM ORDER BY et RANGE BETWEEN interval '1' DAY preceding and current row );
--1天内手机号&身份证号申请次数
create view ad_1 as
select
    et,
    APPLY_TIME,
    CERT_NO,
    MOBILE_NUM,
    count (1) OVER w as cnt
from mayi_info_1
    window w as (partition by MOBILE_NUM,CERT_NO ORDER BY et RANGE BETWEEN interval '1' DAY preceding and current row );


--1天内同一单位名称申请贷款人数
create view e_1 as
select
    et,
    APPLY_TIME,
    COMP_NAME,
    count (1) OVER w as cnt
from mayi_info_1
    window w as (partition by COMP_NAME ORDER BY et RANGE BETWEEN interval '1' DAY preceding and current row );

-- 1天内同一地址申请次数
create view f_1 as
select
    et,
    APPLY_TIME,
    STANDARD_ADDR,
    count (1) OVER w as cnt
from mayi_info_1
    window w as (partition by STANDARD_ADDR ORDER BY et RANGE BETWEEN interval '1' DAY preceding and current row );



-- 选择CERT_NO 本人身份证的view
create view fake_data_1 as
select
    et,
    APPLY_TIME,
    APP_NO,
    CERT_NO,
    'fake' as flag
from mayi_info_1;
-- 选择SPOUSE_CERT_NO 联系人身份证的view
create view real_data_1 as
select
    et,
    APPLY_TIME,
    APP_NO,
    SPOUSE_CERT_NO,
    'real' as flag
from mayi_info_1;

-- 两个view union all 相当于把两个view中的数据合在一起
create view union_view_1 as
select *
from
    (select et,APPLY_TIME,APP_NO,CERT_NO as id,flag from fake_data_1)
union all
     (select et,APPLY_TIME,APP_NO,SPOUSE_CERT_NO as id,flag from real_data_1);

-- 根据union_view 计算次数的时候，注意flag  sum(if(flag='fake',0,1))
create view union_data_1 as
select
    id,
    et,
    APPLY_TIME,
    sum(if(flag='fake',0,1)) over(partition by id order by et range between interval '1' DAY preceding and current row) as cnt
from union_view_1;

-- phone number
create view fake_data_phone_1 as
select
    et,
    APPLY_TIME,
    APP_NO,
    MOBILE_NUM,
    'fake' as flag
from mayi_info_1;

create view real_data_phone_1 as
select
    et,
    APPLY_TIME,
    APP_NO,
    SPOUSE_MOBILE_NO,
    'real' as flag
from mayi_info_1;

create view union_view_phone_1 as
select *
from
    (select et,APPLY_TIME,APP_NO,MOBILE_NUM as phone,flag from fake_data_phone_1)
union all
     (select et,APPLY_TIME,APP_NO,SPOUSE_MOBILE_NO as phone,flag from real_data_phone_1);

create view union_data_phone_1 as
select
    phone,
    et,
    APPLY_TIME,
    sum(if(flag='fake',0,1)) over(partition by phone order by et range between interval '1' DAY preceding and current row) as cnt
from union_view_phone_1;

create view g_1 as
select
    CERT_NO,
    APPLY_TIME,
    et,
    count(distinct SPOUSE_CERT_NO) over(partition by CERT_NO order by et range between interval '1' DAY preceding and current row) as cnt
from mayi_info_1;

create view h_1 as
select
    CERT_NO,
    APPLY_TIME,
    et,
    count(distinct SPOUSE_MOBILE_NO) over(partition by CERT_NO order by et range between interval '1' DAY preceding and current row) as cnt
from mayi_info_1;
-- ===============================================================1day end================================================================================


-- result
create view result_view as
select
    ods.CERT_NO,
    a_1.cnt                     as one_days_cert_apply_num,
    union_data_1.cnt            as one_days_cert_contact_num,
    union_data_phone_1.cnt      as one_days_mobile_contact_num,
    a_1.cnt-ad_1.cnt+d_1.cnt    as one_days_cert_or_mobile_num,
    g_1.cnt                     as one_days_cert_couple_num,
    h_1.cnt                     as one_days_cert_couple_mobile_num,
    e_1.cnt                     as one_days_company_apply_num,
    f_1.cnt                     as one_days_address_apply_num,
    a_3.cnt                     as three_days_cert_apply_num,
    union_data_3.cnt            as three_days_cert_contact_num,
    union_data_phone_3.cnt      as three_days_mobile_contact_num,
    a_3.cnt-ad_3.cnt+d_3.cnt    as three_days_cert_or_mobile_num,
    g_3.cnt                     as three_days_cert_couple_num,
    h_3.cnt                     as three_days_cert_couple_mobile_num,
    e_3.cnt                     as three_days_company_apply_num,
    f_3.cnt                     as three_days_address_apply_num,
    a.cnt                       as seven_days_cert_apply_num,
    union_data.cnt              as seven_days_cert_contact_num,
    union_data_phone.cnt        as seven_days_mobile_contact_num,
    a.cnt-ad.cnt+d.cnt          as seven_days_cert_or_mobile_num,
    g.cnt                       as seven_days_cert_couple_num,
    h.cnt                       as seven_days_cert_couple_mobile_num,
    e.cnt                       as seven_days_company_apply_num,
    f.cnt                       as seven_days_address_apply_num
from mayi_info_1 as ods
join a                    on ods.APPLY_TIME=a.APPLY_TIME                    and ods.CERT_NO=a.CERT_NO                                                and ods.et between a.et -interval '2' second and a.et + interval '2' second
join e                    on ods.APPLY_TIME=e.APPLY_TIME                    and ods.COMP_NAME=e.COMP_NAME                                            and ods.et between e.et -interval '2' second and e.et + interval '2' second
join f                    on ods.APPLY_TIME=f.APPLY_TIME                    and ods.STANDARD_ADDR=f.STANDARD_ADDR                                    and ods.et between f.et -interval '2' second and f.et + interval '2' second
join d                    on ods.APPLY_TIME=d.APPLY_TIME                    and ods.MOBILE_NUM=d.MOBILE_NUM                                          and ods.et between d.et -interval '2' second and d.et + interval '2' second
join ad                   on ods.APPLY_TIME=ad.APPLY_TIME                   and ods.MOBILE_NUM=ad.MOBILE_NUM and ods.CERT_NO=ad.CERT_NO              and ods.et between ad.et -interval '2' second and ad.et + interval '2' second
join union_data           on ods.APPLY_TIME=union_data.APPLY_TIME           and ods.CERT_NO=union_data.id                                            and ods.et between union_data.et -interval '2' second and union_data.et + interval '2' second
join union_data_phone     on ods.APPLY_TIME=union_data_phone.APPLY_TIME     and ods.MOBILE_NUM=union_data_phone.phone                                and ods.et between union_data_phone.et -interval '2' second and union_data_phone.et + interval '2' second
join g                    on ods.APPLY_TIME=g.APPLY_TIME                    and ods.CERT_NO=g.CERT_NO                                                and ods.et between g.et -interval '2' second and g.et + interval '2' second
join h                    on ods.APPLY_TIME=h.APPLY_TIME                    and ods.CERT_NO=h.CERT_NO                                                and ods.et between h.et -interval '2' second and h.et + interval '2' second
join a_1                  on ods.APPLY_TIME=a_1.APPLY_TIME                  and ods.CERT_NO=a_1.CERT_NO                                              and ods.et between a_1.et -interval '2' second and a_1.et + interval '2' second
join e_1                  on ods.APPLY_TIME=e_1.APPLY_TIME                  and ods.COMP_NAME=e_1.COMP_NAME                                          and ods.et between e_1.et -interval '2' second and e_1.et + interval '2' second
join f_1                  on ods.APPLY_TIME=f_1.APPLY_TIME                  and ods.STANDARD_ADDR=f_1.STANDARD_ADDR                                  and ods.et between f_1.et -interval '2' second and f_1.et + interval '2' second
join d_1                  on ods.APPLY_TIME=d_1.APPLY_TIME                  and ods.MOBILE_NUM=d_1.MOBILE_NUM                                        and ods.et between d_1.et -interval '2' second and d_1.et + interval '2' second
join ad_1                 on ods.APPLY_TIME=ad_1.APPLY_TIME                 and ods.MOBILE_NUM=ad_1.MOBILE_NUM and ods.CERT_NO=ad_1.CERT_NO          and ods.et between ad_1.et -interval '2' second and ad_1.et + interval '2' second
join union_data_1         on ods.APPLY_TIME=union_data_1.APPLY_TIME         and ods.CERT_NO=union_data_1.id                                          and ods.et between union_data_1.et -interval '2' second and union_data_1.et + interval '2' second
join union_data_phone_1   on ods.APPLY_TIME=union_data_phone_1.APPLY_TIME   and ods.MOBILE_NUM=union_data_phone_1.phone                              and ods.et between union_data_phone_1.et -interval '2' second and union_data_phone_1.et + interval '2' second
join g_1                  on ods.APPLY_TIME=g_1.APPLY_TIME                  and ods.CERT_NO=g_1.CERT_NO                                              and ods.et between g_1.et -interval '2' second and g_1.et + interval '2' second
join h_1                  on ods.APPLY_TIME=h_1.APPLY_TIME                  and ods.CERT_NO=h_1.CERT_NO                                              and ods.et between h_1.et -interval '2' second and h_1.et + interval '2' second
join a_3                  on ods.APPLY_TIME=a_3.APPLY_TIME                  and ods.CERT_NO=a_3.CERT_NO                                              and ods.et between a_3.et -interval '2' second and a_3.et + interval '2' second
join e_3                  on ods.APPLY_TIME=e_3.APPLY_TIME                  and ods.COMP_NAME=e_3.COMP_NAME                                          and ods.et between e_3.et -interval '2' second and e_3.et + interval '2' second
join f_3                  on ods.APPLY_TIME=f_3.APPLY_TIME                  and ods.STANDARD_ADDR=f_3.STANDARD_ADDR                                  and ods.et between f_3.et -interval '2' second and f_3.et + interval '2' second
join d_3                  on ods.APPLY_TIME=d_3.APPLY_TIME                  and ods.MOBILE_NUM=d_3.MOBILE_NUM                                        and ods.et between d_3.et -interval '2' second and d_3.et + interval '2' second
join ad_3                 on ods.APPLY_TIME=ad_3.APPLY_TIME                 and ods.MOBILE_NUM=ad_3.MOBILE_NUM and ods.CERT_NO=ad_3.CERT_NO          and ods.et between ad_3.et -interval '2' second and ad_3.et + interval '2' second
join union_data_3         on ods.APPLY_TIME=union_data_3.APPLY_TIME         and ods.CERT_NO=union_data_3.id                                          and ods.et between union_data_3.et -interval '2' second and union_data_3.et + interval '2' second
join union_data_phone_3   on ods.APPLY_TIME=union_data_phone_3.APPLY_TIME   and ods.MOBILE_NUM=union_data_phone_3.phone                              and ods.et between union_data_phone_3.et -interval '2' second and union_data_phone_3.et + interval '2' second
join g_3                  on ods.APPLY_TIME=g_3.APPLY_TIME                  and ods.CERT_NO=g_3.CERT_NO                                              and ods.et between g_3.et -interval '2' second and g_3.et + interval '2' second
join h_3                  on ods.APPLY_TIME=h_3.APPLY_TIME                  and ods.CERT_NO=h_3.CERT_NO                                              and ods.et between h_3.et -interval '2' second and h_3.et + interval '2' second;

insert into sink_table select * from result_view;