package com.xy.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @Package com.xy.dwd.UserAgeLabel
 * @Author xinyi.jiao
 * @Date 2025/5/12 10:33
 * @description: 年龄标签
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

        SingleOutputStreamOperator<JSONObject> ds1 = distinctStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject object = new JSONObject();
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
                object.put("birthday", birthday);
                object.put("decade", birthDecade);
                object.put("name", name);
                object.put("birth_decade",birth_decade1);
                object.put("ts_ms", ts_ms);
                object.put("zodiac_sign", zodiacSign);
                object.put("id", id);
                object.put("user_level",user_level);
                object.put("gender", gender);
                object.put("age",age);
                object.put("create_time", createTime);
                object.put("login_name", login_name);
                return object;
            }
        });
        ds1.print();

        SingleOutputStreamOperator<JSONObject> userInfoStream = kafkaStrDS
                .map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("user_info_sup_msg"));
//        userInfoStream.print();
        SingleOutputStreamOperator<JSONObject> ds2 = userInfoStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject object = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");

                Integer height = after.getInteger("height");
                String unit_height = after.getString("unit_height");
                Integer weight = after.getInteger("weight");
                String unit_weight = after.getString("unit_weight");
                Integer uid = after.getInteger("uid");
                object.put("height", height);
                object.put("unit_height", unit_height);
                object.put("weight", weight);
                object.put("unit_weight", unit_weight);
                object.put("uid", uid);
                return object;
            }
        });
     ds1.keyBy(o->o.getInteger("id")).intervalJoin(ds2.keyBy(o->o.getInteger("uid"))).between(Time.seconds(-60), Time.seconds(60));

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
