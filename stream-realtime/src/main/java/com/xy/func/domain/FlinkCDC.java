package com.xy.func.domain;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Package com.xy.dim.app.FlinkCDC
 * @Author xinyi.jiao
 * @Date 2025/5/12 9:13
 * @description: flinkcdc
 */
public class FlinkCDC {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度
        env.setParallelism(4);
        Properties prop = new Properties();
        prop.put("useSSL","false");
        prop.put("decimal.handling.mode","double");
        prop.put("time.precision.mode","connect");
        prop.setProperty("scan.incremental.snapshot.chunk.key-column","id");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.160.60.17")
                .port(3306)
                .debeziumProperties(prop)
                .databaseList("dev_realtime_v1_xinyi_jiao") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("dev_realtime_v1_xinyi_jiao.category_compare_dic") // 设置捕获的表
                .username("root")
                .password("Zh1028,./")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();
        // 设置 3s 的 checkpoint 间隔
        env.enableCheckpointing(3000);
        DataStreamSource<String> mySQLSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        mySQLSource.print();
        //输出的数据
        //{"before":null,"after":{"id":29,"spu_id":10,"price":"RQ==","sku_name":"CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M01醉蔷薇","sku_desc":"CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M01醉蔷薇","weight":"ZA==","tm_id":9,"category3_id":477,"sku_default_img"
        // :"http://47.93.148.192:8080/group1/M00/00/02/rBHu8l-0y1eAS25WAAEYg9IP_7o495.jpg","is_sale":0,"create_time":1639440000000,"operate_time":null},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false",
        // "db":"dev_realtime_v1_xinyi_jiao","sequence":null,"table":"sku_info","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1744032953042,"transaction":null}
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("xinyi_jiao_dia")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        mySQLSource.sinkTo(sink);

        env.execute("flink_cdc");
    }
}
