# Flink SQL模板
## 执行说明
* 执行时指定参数 -w <work-space-dir> -f <sql-file>
    + -w SQL文件路径
    + -f SQL文件名
     
## SQL说明
* 默认流处理
* 支持set, create table, create view, insert into语法
* SQL语法参照[Flink官方文档](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/sql/queries.html "Flink sql queries")

## quick start

```shell
1. 项目打包SQL-Template-1.0-SNAPSHOT.jar上传到flink的lib目录下
2. 写一个test.sql放到一个目录下
3. 运行 bin/flink run -m yarn-cluster ./lib/SQL-Template-1.0-SNAPSHOT.jar -w ./ -f test.sql
4. 启动kafka消费者 bin/kafka-console-consumer --bootstrap-server 10.1.30.8:9092 --topic test_sink
5. 启动kafka生产者 bin/kafka-console-producer --broker-list 10.1.30.8:9092 --topic test_source
6. 发送一条数据 {"id":"001","name":"zhangsan"}可以在kafka消费者端看到相同的数据
```

**test.sql**

```sql
drop function IF EXISTS testfunction ;

CREATE  FUNCTION IF NOT EXISTS testfunction AS 'com.cebbank.airisk.flink.udaf.TestFunction';
create table source_table(
	id string,
	name string
)with(
	'connector.type' = 'kafka',
	'connector.version' = 'universal',
	'connector.topic' = 'test_source',
	'connector.properties.group.id'='dev_flink',
	'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
	'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
	'format.type' = 'json',
	'update-mode' = 'append'
);

-- 创建view
create view view_table as
select id,name from source_table;

create table sink_table(
	id string,
	name string
)with(
	'connector.type' = 'kafka',
	'connector.version' = 'universal',
	'connector.topic' = 'test_sink',
	'connector.properties.group.id'='dev_flink',
	'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
	'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
	'format.type' = 'json',
	'update-mode' = 'append'
);

insert into sink_table
select * from view_table;
```

## 目前的问题

flink 1.10.0有注册函数的bug,如果想要注册函数可以在代码中注册，使用sql注册可能会报错！
```java
	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final CliOptions options = CliOptionsParser.parseClient(args);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useBlinkPlanner()
				.build();
		StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

		// 注册函数
		tableEnvironment.registerFunction("previous_lag_long",new PreviousValueAggFunction.LongPreviousValueAggFunction());

		SqlSubmit submit=new SqlSubmit(options,tableEnvironment);
		submit.run();

	}
```