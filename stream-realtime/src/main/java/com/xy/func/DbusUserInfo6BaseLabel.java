package com.xy.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xy.constant.Constant;
import com.xy.func.domain.DimBaseCategory;
import com.xy.func.domain.JdbcUtils;
import com.xy.stram.utlis.AggregateUserDataProcessFunction;
import com.xy.stram.utlis.MapDeviceAndSearchMarkModelFunc;
import com.xy.stram.utlis.MapDeviceInfoAndSearchKetWordMsg;
import com.xy.util.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Connection;
import java.util.Date;
import java.util.List;

/**
 * @Package com.xy.dwd.app.DbusUserInfo6BaseLabel
 * @Author xinyi.jiao
 * @Date 2025/5/14 18:58
 * @description: 日志 log
 */
public class DbusUserInfo6BaseLabel {
    private static final List<DimBaseCategory> dim_base_categories;
    private static final Connection connection;

    private static final double device_rate_weight_coefficient = 0.1; // 设备权重系数
    private static final double search_rate_weight_coefficient = 0.15; // 搜索权重系数
    static {
        try {
            connection = JdbcUtils.getMySQLConnection(
                    Constant.MYSQL_URL,
                    Constant.MYSQL_USER_NAME,
                    Constant.MYSQL_PASSWORD);
            String sql = "select b3.id,                          \n" +
                    "            b3.name as b3name,              \n" +
                    "            b2.name as b2name,              \n" +
                    "            b1.name as b1name               \n" +
                    "     from realtime_v1.base_category3 as b3  \n" +
                    "     join realtime_v1.base_category2 as b2  \n" +
                    "     on b3.category2_id = b2.id             \n" +
                    "     join realtime_v1.base_category1 as b1  \n" +
                    "     on b2.category1_id = b1.id";
            dim_base_categories = JdbcUtils.queryList2(connection, sql, DimBaseCategory.class, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        System.setProperty("HADOOP_USER_NAME", "root");
        //TODO 读取日志数据
        SingleOutputStreamOperator<String> kafkaPageLogSource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        Constant.KAFKA_BROKERS,
                        Constant.TOPIC_PAGE_LOG,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "fact_log_source"
        ).uid("fact_log_source").name("fact_log_source");
// 转为json
        SingleOutputStreamOperator<JSONObject> kafkaCdcobj = kafkaPageLogSource.map(JSON::parseObject)
         .uid("convert json page log")
         .name("convert json page log");
//        kafkaCdcobj.print();
        //提取字段
        SingleOutputStreamOperator<JSONObject> MaoDevicInfo = kafkaCdcobj.map(new MapDeviceInfoAndSearchKetWordMsg())
                .uid("filter_log_Intercept")
                .name("filter_log_Intercept");
//        MaoDevicInfo.print();
//        {"uid":"20","os":"iOS,Android","ch":"Appstore,xiaomi","pv":14,"md":"iPhone 14,vivo x90,xiaomi 12 ultra ","search_item":"","ba":"iPhone,xiaomi,vivo","ts":1746980053228}
        //key
        SingleOutputStreamOperator<JSONObject> filterNotNUllUidLogPage = MaoDevicInfo.filter(data -> !data.getString("uid").isEmpty());
        KeyedStream<JSONObject, String> keyedSteamLogPage = filterNotNUllUidLogPage.keyBy(data -> data.getString("uid"));
//        keyedSteamLogPage.print();
        //去重
        SingleOutputStreamOperator<JSONObject> singleOutputStreamOperator = keyedSteamLogPage.filter(new FilterBloomDeduplicatorFunc(1000000, 0.01, "uid", "ts"))
                .uid("deduplication_log_bl")
                .name("deduplication_log_bl");
//        singleOutputStreamOperator.print();
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = singleOutputStreamOperator.keyBy(data -> data.getString("uid"))
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .reduce((value1, value2) -> value2)
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");
// 设备打分模型
        win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories,device_rate_weight_coefficient,search_rate_weight_coefficient))
                .print();


        env.execute();
    }
}
