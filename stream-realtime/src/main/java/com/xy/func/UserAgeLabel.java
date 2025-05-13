package com.xy.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xy.constant.Constant;
import com.xy.stram.utlis.MapDeviceInfoAndSearchKetWordMsg;
import com.xy.util.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @Package com.xy.dwd.UserAgeLabel
 * @Author xinyi.jiao
 * @Date 2025/5/12 10:33
 * @description: 基础特征
 */
public class UserAgeLabel {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


         //读取kafa数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("xinyi_jiao_yw")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        {"common":{"ar":"3","uid":"638","os":"Android 13.0","ch":"xiaomi","is_new":"0","md":"realme Neo2","mid":"mid_17","vc":"v2.1.134","ba":"realme","sid":"c6111002-3d81-4ecb-bba5-658c29d00c47"},"page":{"page_id":"payment","item":"2257","during_time":10736,"item_type":"order_id","last_page_id":"order"},"ts":1743864652487}
        //TODO 获取 user_info
        SingleOutputStreamOperator<JSONObject> kafkaJson = kafkaStrDS
                .map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("user_info"));

        SingleOutputStreamOperator<JSONObject> orderInfoStream = kafkaJson.map(jsonStr -> {
            JSONObject json = JSON.parseObject(String.valueOf(jsonStr));
            JSONObject after = json.getJSONObject("after");
            if (after != null && after.containsKey("birthday")) {
                Integer epochDay = after.getInteger("birthday");
                if (epochDay != null) {
                    LocalDate date = LocalDate.ofEpochDay(epochDay);
                    after.put("birthday", date.format(DateTimeFormatter.ISO_DATE));
                        // 添加星座判断逻辑
                    String zodiacSign = getZodiacSign(date);
                    after.put("zodiac_sign", zodiacSign);
                    // 添加年代字段
                    int year = date.getYear();
                    int decade = (year / 10) * 10; // 计算年代（如1990, 2000）
                    after.put("birth_decade", decade);
                    // 添加年龄计算逻辑
                    LocalDate currentDate = LocalDate.now();
                    int age = calculateAge(date, currentDate);
                    after.put("age", age);
                }
            }
            return json;
        });
//        orderInfoStream.print();
        SingleOutputStreamOperator<JSONObject> distinctStream = orderInfoStream
                .keyBy(json -> json.getJSONObject("after").getString("id"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private transient ValueState<Boolean> seenState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化状态：用于标记该ID是否已处理过
                        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>(
                                "seenId", // 状态名称
                                Boolean.class // 状态类型
                        );
                        seenState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(
                            JSONObject json,
                            Context ctx,
                            Collector<JSONObject> out
                    ) throws Exception {
                        // 检查是否已处理过该ID
                        Boolean seen = seenState.value();
                        if (seen == null || !seen) {
                            // 如果未处理过，则输出数据并更新状态
                            out.collect(json);
                            seenState.update(true);
                        }
                    }
                });

//        distinctStream.print();
           //提取字段
        SingleOutputStreamOperator<JSONObject> ds1 = distinctStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");
                Integer birth_decade1 = after.getInteger("birth_decade");
                Integer age = after.getInteger("age");
                Integer user_level = after.getInteger("user_level");
                Long ts_ms = after.getLong("ts_ms");
                String birthday = after.getString("birthday");
                String gender = after.getString("gender");
                String name = after.getString("name");
                String zodiacSign = after.getString("zodiac_sign");
                String createTime = after.getString("create_time");
                Integer id = after.getInteger("id");
                String birthDecade = after.getString("decade");
                String login_name = after.getString("login_name");
                result.put("birthday", birthday);
                result.put("decade", birthDecade);
                result.put("name", name);
                result.put("birth_decade",birth_decade1);
                result.put("ts_ms", ts_ms);
                result.put("zodiac_sign", zodiacSign);
                result.put("id", id);
                result.put("user_level",user_level);
                result.put("gender", gender);
                result.put("age",age);
                result.put("create_time", createTime);
                result.put("login_name", login_name);
                return result;
            }
        });
            //  ds1.print();
       //TODO 获取 user_info_sup_msg
        SingleOutputStreamOperator<JSONObject> userInfoStream = kafkaStrDS
                .map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("user_info_sup_msg"));
                // userInfoStream.print();
        SingleOutputStreamOperator<JSONObject> ds2 = userInfoStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");
                Integer height = after.getInteger("height");
                String unit_height = after.getString("unit_height");
                Integer weight = after.getInteger("weight");
                String unit_weight = after.getString("unit_weight");
                Integer uid = after.getInteger("uid");
                result.put("height", height);
                result.put("unit_height", unit_height);
                result.put("weight", weight);
                result.put("unit_weight", unit_weight);
                result.put("uid", uid);
                return result;
            }
        });
        // 关联
        SingleOutputStreamOperator<JSONObject> ds3 = ds1.keyBy(o -> o.getInteger("id"))
        .intervalJoin(ds2.keyBy(o -> o.getInteger("uid")))
        .between(Time.seconds(-60), Time.seconds(60))
        .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                jsonObject.putAll(jsonObject2);
                collector.collect(jsonObject);
            }
        });
//        ds3.print();


        //TODO 提取表 order_detail
        SingleOutputStreamOperator<JSONObject> orderDetailStream = kafkaStrDS
                .map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("order_detail"));
        SingleOutputStreamOperator<JSONObject> orderDetailStream1 = orderDetailStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");

                Integer id = after.getInteger("id");
                Integer order_id = after.getInteger("order_id");
                Integer sku_id = after.getInteger("sku_id");
                String sku_name = after.getString("sku_name");
                Double order_price = after.getDouble("order_price");
                Integer sku_num = after.getInteger("sku_num");
                Long createTime = after.getLong("create_time");
                result.put("id", id);
                result.put("order_id", order_id);
                result.put("sku_id", sku_id);
                result.put("sku_name",sku_name);
                result.put("order_price", order_price);
                result.put("sku_num", sku_num);
                result.put("create_time", createTime);
                return result;
            }
        });
                // orderDetailStream1.print();
        //TODO 获取 spu_info
        SingleOutputStreamOperator<JSONObject> SpuInfoStream = kafkaStrDS
                .map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("spu_info"));
        SingleOutputStreamOperator<JSONObject> spuInfoStream = SpuInfoStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");
                Integer id = after.getInteger("id");
                Integer category3_id = after.getInteger("category3_id");
                Integer tm_id = after.getInteger("tm_id");
                result.put("tm_id",tm_id);
                result.put("id", id);
                result.put("category3_id", category3_id);
                return result;
            }
        });
        //关联
        SingleOutputStreamOperator<JSONObject> ds4 = orderDetailStream1.keyBy(o -> o.getInteger("sku_id"))
                .intervalJoin(spuInfoStream.keyBy(o -> o.getInteger("id")))
                .between(Time.seconds(-60), Time.seconds(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        jsonObject.putAll(jsonObject2);
                        collector.collect(jsonObject);
                    }
                });
        //TODO 读取日志数据
        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        Constant.KAFKA_BROKERS,
                        Constant.TOPIC_LOG,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "fact_log_source"
        ).uid("fact_log_source").name("fact_log_source");
// 转为json
        SingleOutputStreamOperator<JSONObject> kafkaCdcobj = kafkaCdcDbSource.map(JSON::parseObject);
//        kafkaCdcobj.print();
        //提取字段
        SingleOutputStreamOperator<JSONObject> MaoDevicInfo = kafkaCdcobj.map(new MapDeviceInfoAndSearchKetWordMsg())
                .uid("filter_log_Intercept")
                .name("filter_log_Intercept");
//        MaoDevicInfo.print();
        //key
        SingleOutputStreamOperator<JSONObject> filterNotNUllUidLogPage = MaoDevicInfo.filter(data -> !data.getString("uid").isEmpty());
        KeyedStream<JSONObject, String> keyedSteamLogPage = filterNotNUllUidLogPage.keyBy(data -> data.getString("uid"));
        //去重
        SingleOutputStreamOperator<JSONObject> singleOutputStreamOperator = keyedSteamLogPage.filter(new FilterBloomDeduplicatorFunc(1000000, 0.01, "uid", "ts"))
                .uid("deduplication_log_bl")
                .name("deduplication_log_bl");
//        singleOutputStreamOperator.print();


        env.execute();
    }
    private static String getZodiacSign(LocalDate date) {
        int month = date.getMonthValue();
        int day = date.getDayOfMonth();

// 定义星座区间映射
        if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) {
            return "摩羯座";
        } else if ((month == 1 && day >= 20) || (month == 2 && day <= 18)) {
            return "水瓶座";
        } else if ((month == 2 && day >= 19) || (month == 3 && day <= 20)) {
            return "双鱼座";
        } else if ((month == 3 && day >= 21) || (month == 4 && day <= 19)) {
            return "白羊座";
        } else if ((month == 4 && day >= 20) || (month == 5 && day <= 20)) {
            return "金牛座";
        } else if ((month == 5 && day >= 21) || (month == 6 && day <= 21)) {
            return "双子座";
        } else if ((month == 6 && day >= 22) || (month == 7 && day <= 22)) {
            return "巨蟹座";
        } else if ((month == 7 && day >= 23) || (month == 8 && day <= 22)) {
            return "狮子座";
        } else if ((month == 8 && day >= 23) || (month == 9 && day <= 22)) {
            return "处女座";
        } else if ((month == 9 && day >= 23) || (month == 10 && day <= 23)) {
            return "天秤座";
        } else if ((month == 10 && day >= 24) || (month == 11 && day <= 22)) {
            return "天蝎座";
        } else if ((month == 11 && day >= 23) || (month == 12 && day <= 21)) {
            return "射手座";
        }
        return "未知"; // 默认情况，实际上不会执行到这一步
    }
    private static int calculateAge(LocalDate birthDate, LocalDate currentDate) {
// 如果生日日期晚于当前日期，抛出异常
        if (birthDate.isAfter(currentDate)) {
            throw new IllegalArgumentException("生日日期不能晚于当前日期");
        }

        int age = currentDate.getYear() - birthDate.getYear();

// 如果当前月份小于生日月份，或者月份相同但日期小于生日日期，则年龄减1
        if (currentDate.getMonthValue() < birthDate.getMonthValue() ||
                (currentDate.getMonthValue() == birthDate.getMonthValue() &&
                        currentDate.getDayOfMonth() < birthDate.getDayOfMonth())) {
            age--;
        }

        return age;
    }
}
