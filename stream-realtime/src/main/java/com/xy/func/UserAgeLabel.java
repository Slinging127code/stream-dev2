package com.xy.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jiao.util.HBaseUtil;
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
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @Package com.xy.dwd.app.UserAgeLabel
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

        SingleOutputStreamOperator<JSONObject> userInfoStream = kafkaJson.map(jsonStr -> {
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
//        userInfoStream.print();
        SingleOutputStreamOperator<JSONObject> distinctStream = userInfoStream
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
        SingleOutputStreamOperator<JSONObject> userInfoSuoStream = kafkaStrDS
                .map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("user_info_sup_msg"));
                // userInfoStream.print();
        SingleOutputStreamOperator<JSONObject> ds2 = userInfoSuoStream.map(new RichMapFunction<JSONObject, JSONObject>() {

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
        // TODO 关联 user_info user_info_sup_msg
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
//        {"birthday":"1997-10-02","create_time":"1746227166000","zodiac_sign":"天秤座","weight":41,"birth_decade":1990,"uid":408,"login_name":"didsys","unit_height":"cm","name":"东郭芳","user_level":1,"id":408,"unit_weight":"kg","age":27,"height":180}
//        ds3.print();

        //TODO 提取表 order_detail
        SingleOutputStreamOperator<JSONObject> orderDetailStream = kafkaStrDS
                .map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("order_detail"));
//        orderDetailStream.print("orderDetailStream====>");
//        SingleOutputStreamOperator<JSONObject> BloomDetail  = orderDetailStream.keyBy(o -> o.getJSONObject("after").getInteger("id"))
//                .filter(new PublicFilterBloomFunc(10 * 1024 * 1024, 0.0001, "id", "ts_ms"))
//                .uid("Filter detail Bloom").name("Filter detail Bloom");
        //TODO 获取 order_info
        SingleOutputStreamOperator<JSONObject> orderInfoStream = kafkaStrDS
                .map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("order_info"));

//        SingleOutputStreamOperator<JSONObject> BloomInfo = orderInfoStream.keyBy(o -> o.getJSONObject("after").getInteger("id"))
//                .filter(new PublicFilterBloomFunc(10 * 1024 * 1024, 0.0001, "id", "ts_ms"))
//                .uid("Filter order Bloom").name("Filter order Bloom");
        //TODO 关联 order_detail order_info
        SingleOutputStreamOperator<JSONObject> ds4 = orderDetailStream.keyBy(data -> data.getJSONObject("after").getInteger("order_id")).intervalJoin(orderInfoStream.keyBy(data -> data.getJSONObject("after").getInteger("id")))
                .between(Time.seconds(-60), Time.seconds(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject detail, JSONObject info, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject result = new JSONObject();
                        result.put("id", detail.getJSONObject("after").getInteger("id"));
                        result.put("order_id", detail.getJSONObject("after").getInteger("order_id"));
                        result.put("sku_id", detail.getJSONObject("after").getInteger("sku_id"));
                        result.put("sku_name", detail.getJSONObject("after").getString("sku_name"));
                        result.put("sku_num", detail.getJSONObject("after").getInteger("sku_num"));
                        result.put("create_time", info.getJSONObject("after").getString("create_time"));
                        result.put("user_id", info.getJSONObject("after").getInteger("user_id"));
                        result.put("total_amount", info.getJSONObject("after").getBigDecimal("total_amount"));
                        result.put("original_total_amount", info.getJSONObject("after").getBigDecimal("original_total_amount"));
                        result.put("ts_ms", info.getLong("ts_ms"));
                        collector.collect(result);
                    }
                });
//        {"sku_num":1,"create_time":"1746529711000","user_id":118,"total_amount":6296.0,"sku_id":22,"sku_name":"十月稻田 长粒香大米 东北大米 东北香米 5kg","id":1392,"order_id":794}
//        ds4.print();
        //TODO 获取 dim_sku_info
        SingleOutputStreamOperator<JSONObject> skuK = ds4.map(
                new RichMapFunction<JSONObject, JSONObject>() {


                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("sku_id");
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, "ns_xinyi_jiao", "dim_sku_info", skuId, JSONObject.class);
                        JSONObject object = new JSONObject();
                        object.putAll(jsonObject);
                        object.put("sku_name", skuInfoJsonObj.getString("sku_name"));
                        object.put("tm_id", skuInfoJsonObj.getString("tm_id"));
                        object.put("category3_id", skuInfoJsonObj.getString("category3_id"));
                        return object;
                    }

                }
        );
//        {"sku_num":1,"create_time":"1746567437000","tm_id":"8","user_id":129,"total_amount":8370.0,"sku_id":28,"sku_name":"索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 Z03女王红 性感冷艳 璀璨金钻哑光唇膏 ","id":882,"order_id":540,"category3_id":"477"}
//        skuK.print();
        // TODO 关联Hbase BaseCategory3
        SingleOutputStreamOperator<JSONObject> BaseCategory3 = skuK.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("category3_id");
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, "ns_xinyi_jiao", "dim_base_category3", skuId, JSONObject.class);
                        JSONObject a = new JSONObject();
                        a.putAll(jsonObject);
                        a.put("category3_name", skuInfoJsonObj.getString("name"));
                        a.put("category2_id", skuInfoJsonObj.getString("category2_id"));
                        return a;
                    }
                }
        );
//        BaseCategory3.print();
        // TODO 关联Hbase BaseCategory2
        SingleOutputStreamOperator<JSONObject> BaseCategory2 = BaseCategory3.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("category2_id");
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, "ns_xinyi_jiao", "dim_base_category2", skuId, JSONObject.class);
                        JSONObject a = new JSONObject();
                        a.putAll(jsonObject);
                        a.put("category2_name", skuInfoJsonObj.getString("name"));
                        a.put("category1_id", skuInfoJsonObj.getString("category1_id"));

                        return a;
                    }
                }
        );
//        BaseCategory2.print();
//        {"category2_name":"手机通讯","sku_num":1,"create_time":"1747080392000","sku_id":5,"category1_id":"2","tm_id":"1","user_id":404,"total_amount":18297.1,"sku_name":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 4GB+128GB 明月灰 游戏智能手机 小米 红米","id":2790,"category3_name":"手机","order_id":1449,"category3_id":"61","category2_id":"13"}
        // TODO 关联Hbase BaseCategory1
        SingleOutputStreamOperator<JSONObject> BaseCategory1 = BaseCategory2.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("category1_id");
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, "ns_xinyi_jiao", "dim_base_category1", skuId, JSONObject.class);
                        JSONObject a = new JSONObject();
                        a.putAll(jsonObject);
                        a.put("category1_name", skuInfoJsonObj.getString("name"));

                        return a;
                    }
                }
        );
//        BaseCategory1.print();
//        {"category2_name":"手机通讯","sku_num":1,"create_time":"1746199500000","sku_id":8,"category1_id":"2","tm_id":"2","user_id":379,"total_amount":24673.1,"category1_name":"手机","sku_name":"Apple iPhone 12 (A2404) 64GB 黑色 支持移动联通电信5G 双卡双待手机","id":2304,"category3_name":"手机","order_id":1219,"category3_id":"61","ts_ms":1747050794613,"category2_id":"13"}
        //TODO 获取 base_trademark
        SingleOutputStreamOperator<JSONObject> TrademarkSteam = BaseCategory1.map(
                new RichMapFunction<JSONObject, JSONObject>() {


                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("tm_id");
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, "ns_xinyi_jiao", "dim_base_trademark", skuId, JSONObject.class);
                        JSONObject object = new JSONObject();
                        object.putAll(jsonObject);
                        object.put("tm_name", skuInfoJsonObj.getString("tm_name"));

                        return object;
                    }
                }
        );
//        TrademarkSteam.print();
//        {"category2_name":"电脑整机","sku_num":1,"create_time":"1747078188000","sku_id":15,"original_total_amount":61931.0,"category1_id":"6","tm_name":"联想","tm_id":"3","user_id":88,"total_amount":59531.2,"category1_name":"电脑办公","sku_name":"联想（Lenovo） 拯救者Y9000P 2022 16英寸游戏笔记本电脑 i7-12700H 512G RTX3060 钛晶灰","id":2760,"category3_name":"游戏本","order_id":1431,"category3_id":"287","ts_ms":1747050794618,"category2_id":"33"}
        SingleOutputStreamOperator<JSONObject> Generaltable = ds3.keyBy(data -> data.getString("uid")).intervalJoin(TrademarkSteam.keyBy(data -> data.getString("user_id")))
                .between(Time.seconds(-30), Time.seconds(30))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject order, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject result = new JSONObject();
                        Integer height = jsonObject.getInteger("height");
                        String unit_height = jsonObject.getString("unit_height");
                        Integer weight = jsonObject.getInteger("weight");
                        String unit_weight = jsonObject.getString("unit_weight");
                        Integer uid = jsonObject.getInteger("uid");
                        result.put("height", height);
                        result.put("unit_height", unit_height);
                        result.put("weight", weight);
                        result.put("unit_weight", unit_weight);
                        result.put("uid", uid);
                        Integer birth_decade1 = jsonObject.getInteger("birth_decade");
                        Integer age = jsonObject.getInteger("age");
                        Integer user_level = jsonObject.getInteger("user_level");
                        Long ts_ms = jsonObject.getLong("ts_ms");
                        String birthday = jsonObject.getString("birthday");
                        String gender = jsonObject.getString("gender");
                        String name = jsonObject.getString("name");
                        String zodiacSign = jsonObject.getString("zodiac_sign");
                        String createTime = jsonObject.getString("create_time");
                        Integer id = jsonObject.getInteger("id");
                        String birthDecade = jsonObject.getString("decade");
                        String login_name = jsonObject.getString("login_name");
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
                        JSONObject result1 = new JSONObject();
                        result1.put("id", order.getInteger("id"));
                        result1.put("order_id", order.getInteger("order_id"));
                        result1.put("sku_id", order.getInteger("sku_id"));
                        result1.put("sku_name", order.getString("sku_name"));
                        result1.put("sku_num", order.getInteger("sku_num"));
                        result1.put("create_time", order.getString("create_time"));
                        result1.put("user_id", order.getInteger("user_id"));
                        result1.put("total_amount", order.getBigDecimal("total_amount"));
                        result1.put("original_total_amount", order.getBigDecimal("original_total_amount"));
                        result1.put("ts_ms", order.getLong("ts_ms"));
                        result1.put("category2_id", order.getString("category2_id"));
                        result1.put("category3_id", order.getString("category3_id"));
                        result1.put("category1_id", order.getString("category1_id"));
                        result1.put("tm_name", order.getString("tm_name"));
                        result1.put("category_name", order.getString("category_name"));
                        result1.put("os", order.getString("os"));
                        JSONObject result2 = new JSONObject();
                        result2.put("Age",result1);
                        result2.put("user",result);
                        collector.collect(result2);
//                        jsonObject.putAll(order);
//                        collector.collect(jsonObject);
                    }
                });
        Generaltable.print();
//        {"user":{"birthday":"1998-09-06","create_time":"1746533280000","zodiac_sign":"处女座","weight":81,"uid":58,"birth_decade":1990,"login_name":"erh0y4q","unit_height":"cm","name":"孔蓉","user_level":1,"id":58,"unit_weight":"kg","age":26,"height":166},"Age":{"sku_num":1,"create_time":"1746569080000","sku_id":1,"original_total_amount":8367.0,"category1_id":"2","tm_name":"小米","user_id":58,"total_amount":7867.0,"sku_name":"小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机","id":241,"order_id":163,"category3_id":"61","ts_ms":1747050794587,"category2_id":"13"}}




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
