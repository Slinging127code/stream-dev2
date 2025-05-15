package com.xy.func.domain;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xy.constant.Constant;
import com.xy.stram.utlis.AgeScoreFunc;
import com.xy.stram.utlis.JudgmentFunc;
import com.xy.stram.utlis.LogFilterFunc;
import com.xy.util.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;

/**
 * @Package com.xy.dwd.app.DbusUserInfo6BaseLabel
 * @Author xinyi.jiao
 * @Date 2025/5/14 18:58
 * @description: 日志 log
 */
public class DbusUserInfo6BaseLabel {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.1 开启检查点
        //2.2 设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 设置job取消后检查点是否保留
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.days(30), org.apache.flink.api.common.time.Time.seconds(3)));
        //设置状态后端及其检查点储存路径
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/2207A/xinyi_jiao/flink");

        System.setProperty("HADOOP_USER_NAME", "root");
        //读取 category_compare_dic
        SingleOutputStreamOperator<String> kafkaCopareDic = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        Constant.KAFKA_BROKERS,
                        Constant.TOPIC_DIC,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "fact_dic_source"
        ).uid("fact_dic_source").name("fact_dic_source");
// 转为json
        SingleOutputStreamOperator<JSONObject> CopareDic = kafkaCopareDic.map(JSON::parseObject)
                .uid("convert json page dic")
                .name("convert json page dic");
//        CopareDic.print();
        // 过滤出 category_compare_dic 表
        // {"op":"c","after":{"category_name":"保健品","search_category":"健康与养生","id":8},"source":{"thread":161,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000036","connector":"mysql","pos":2310633,"name":"mysql_binlog_source","row":0,"ts_ms":1747222192000,"snapshot":"false","db":"gmall2024","table":"category_compare_dic"},"ts_ms":1747222191269}
        SingleOutputStreamOperator<JSONObject> CategoryCompare = CopareDic
                .filter(data -> data.getJSONObject("source").getString("table").equals("category_compare_dic") && data.getJSONObject("after") != null)
                .uid("filter_category_compare_dic").name("filter_category_compare_dic");
//        CategoryCompare.print("CategoryCompare-->");


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
        SingleOutputStreamOperator<JSONObject> pageLog  = kafkaCdcobj.map(new LogFilterFunc());
//        pageLog.print();

        SingleOutputStreamOperator<JSONObject> paglognull = pageLog.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return jsonObject.getString("uid") != null;
            }
        });
//        paglognull.print();

//        {"uid":"738","os":"Android","ts":1747235256775}
        // 去重
        SingleOutputStreamOperator<JSONObject> filter = paglognull.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                if (jsonObject.containsKey("keyword") && !jsonObject.getString("keyword").isEmpty()){
                    return true;
                }
                return false;
            }
        });
//        filter.print("keyword====>");
        // {"uid":"247","os":"iOS","keyword":"书籍","ts":1747031731000}
        //布隆过滤
//        SingleOutputStreamOperator<JSONObject> BloomLog = pageLog.keyBy(o -> o.getString("uid"))
//                .filter(new PublicFilterBloomFunc(1000000, 0.0001, "uid", "ts"))
//                .uid("Filter Log Bloom").name("Filter Log Bloom");
        //TODO 读取宽表数据
        SingleOutputStreamOperator<String> kafkaPageLogSource1 = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        Constant.KAFKA_BROKERS,
                        Constant.TOPIC_ALL,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "fact_all_source"
        ).uid("fact_all_source").name("fact_all_source");
        // 转为json
        SingleOutputStreamOperator<JSONObject> kafkaCdcobj1 = kafkaPageLogSource1.map(JSON::parseObject)
                .uid("convert json all")
                .name("convert json all");
        SingleOutputStreamOperator<JSONObject> joinAll = kafkaCdcobj1.keyBy(o -> o.getJSONObject("Age").getInteger("user_id"))
                .intervalJoin(filter.keyBy(o -> o.getInteger("uid")))
                .between(Time.minutes(-60), Time.minutes(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector)   {
                        try {
                            JSONObject age = jsonObject.getJSONObject("Age");
                            age.put("os", jsonObject2.getString("os"));
                            age.put("keyword", jsonObject2.getString("keyword"));
                            age.put("ts", jsonObject2.getLong("ts"));
                            collector.collect(jsonObject);
                        }catch (Exception e){
                            e.printStackTrace();
                            System.out.println("报错信息 " +  jsonObject);
                        }
                    }
                }).setParallelism(1).uid("intervalJoin new & bloomLog").name("intervalJoin new & bloomLog");
//        joinAll.print("joinAll-->");


        // 对时间段和价格敏感度做处理
        // {"user":{"birthday":"2007-01-09","create_time":1746790830000,"gender":"M","weight":"60","ageGroup":"18-24","constellation":"摩羯座","unit_height":"cm","Era":"2000年代","name":"东郭宁","id":955,"unit_weight":"kg","ts_ms":1747019493933,"age":18,"height":"183"},"Age":{"category_name":"个护化妆","create_time":1746790919000,"os":"Android","sku_id":30,"price_sensitive":"高价商品","tm_name":"CAREMiLLE","category1_id":8,"tm_id":9,"user_id":955,"total_amount":21667.2,"sku_name":"CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M02干玫瑰","id":5041,"order_id":3588,"category3_id":477,"category2_id":54,"time_period":"晚上","ts":1747031731000}}
        SingleOutputStreamOperator<JSONObject> timePriceMap = joinAll.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject)   {
                if (jsonObject.getJSONObject("Age") != null) {
                    JSONObject age = jsonObject.getJSONObject("Age");

                    // 时间段
                    age.put("time_period", JudgmentFunc.timePeriodJudgment(age.getLong("create_time")));

                    // 价格敏感度
                    age.put("price_sensitive", JudgmentFunc.priceCategoryJudgment(age.getDouble("original_total_amount")));
                }
                return jsonObject;
            }
        }).setParallelism(1).uid("timePriceMap").name("timePriceMap");
//        timePriceMap.print("timePriceMap-->");
//        CategoryCompare.print("====>");
        // 关联搜索词分类表
        // {"user":{"birthday":"1997-07-15","gender":"F","weight":"53","ageGroup":"25-29","constellation":"巨蟹座","unit_height":"cm","Era":"1990年代","name":"蒋枝思","id":338,"unit_weight":"kg","ts_ms":1747221450430,"age":26,"height":"154"},"Age":{"category_name":"个护化妆","create_time":1747248448000,"os":"Android","sku_id":28,"price_sensitive":"高价商品","tm_name":"索芙特","category1_id":8,"tm_id":8,"user_id":338,"total_amount":8596.0,"id":5377,"keyword":"智能手机","order_id":3827,"category3_id":477,"category2_id":54,"time_period":"凌晨","ts":1747221577000},"keyword_type":"科技与数码"}
        SingleOutputStreamOperator<JSONObject> joinSearchCate = timePriceMap.keyBy(o -> o.getJSONObject("Age").getString("keyword"))
                .intervalJoin(CategoryCompare.keyBy(o -> o.getJSONObject("after").getString("category_name")))
                .between(Time.days(-3), Time.days(3))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector)  {
                        if (jsonObject == null || jsonObject2 == null) {
                            return; // 或者记录日志
                        }

                        JSONObject age = jsonObject.getJSONObject("Age");
                        if (age == null) {
                            return;
                        }

                        JSONObject after = jsonObject2.getJSONObject("after");
                        if (after == null || !after.containsKey("search_category")) {
                            return;
                        }

                        String searchCategory = after.getString("search_category");
                        if (searchCategory == null) {
                            return;
                        }

                        try {
                            // 从after对象中获取category_name
                            String categoryName = after.getString("category_name");
                            // 将category_name添加到Age对象中
                            age.put("category_name", categoryName);
                            // 原有逻辑保持不变
                            age.put("keyword_type", searchCategory);
                            collector.collect(jsonObject);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.println("报错信息 " + searchCategory);
                        }
                    }
                })
                .setParallelism(1)
                .uid("intervalJoin new & searchCate")
                .name("intervalJoin new & searchCate");

//        joinSearchCate.print("joinSearchCate-->");

        SingleOutputStreamOperator<JSONObject> mapped = joinSearchCate.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                // 获取用户对象
                JSONObject user = jsonObject.getJSONObject("user");
                if (user != null) {
                    Integer age = user.getInteger("age");
                    String ageRange = JudgmentFunc.ageJudgment(age);
                    user.put("ageGroup", ageRange);
                }

                return jsonObject;
            }
        });
//        {"user":{"birthday":"1980-07-08","gender":"F","create_time":"1717804800000","zodiac_sign":"巨蟹座","weight":51,"ageGroup":"40-49","uid":22,"birth_decade":1980,"login_name":"g6jqg3vzq9","unit_height":"cm","name":"熊亚宜","user_level":1,"id":22,"unit_weight":"kg","age":44,"height":153},"Age":{"category_name":"小米","sku_num":1,"create_time":"1746538068000","os":"Android","sku_id":29,"price_sensitive":"高价商品","original_total_amount":21694.0,"keyword_type":"性价比","category1_id":"8","tm_name":"CAREMiLLE","user_id":22,"total_amount":21444.0,"category1_name":"个护化妆","sku_name":"CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M01醉蔷薇","id":84,"keyword":"小米","order_id":58,"category3_id":"477","ts_ms":1747050794583,"category2_id":"54","time_period":"晚上","ts":1747055439947}}
//        mapped.print();
        SingleOutputStreamOperator<JSONObject> filterstr = mapped.filter(o -> o.getJSONObject("user").getString("ageGroup").equals("18-24"));
//        {"user":{"birthday":"2005-07-06","gender":"M","create_time":"1746528898000","zodiac_sign":"巨蟹座","weight":62,"ageGroup":"18-24","uid":230,"birth_decade":2000,"login_name":"g60o358w6i6t","unit_height":"cm","name":"苏涛","user_level":1,"id":230,"unit_weight":"kg","age":19,"height":171},"Age":{"category_name":"xiaomiSu7","sku_num":1,"create_time":"1746574268000","os":"Android","sku_id":3,"price_sensitive":"高价商品","original_total_amount":29806.0,"keyword_type":"家庭与育儿","category1_id":"2","tm_name":"小米","user_id":230,"total_amount":28886.1,"category1_name":"手机","sku_name":"小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机","id":1297,"keyword":"xiaomiSu7","order_id":753,"category3_id":"61","ts_ms":1747050794602,"category2_id":"13","time_period":"早晨","ts":1747004587863}}
//        filterstr.print();
        // 计算各个维度的分数
        SingleOutputStreamOperator<JSONObject> AgeEachScore = filterstr.map(new AgeScoreFunc());
//        {"user":{"birthday":"2005-07-06","gender":"M","create_time":"1746528898000","zodiac_sign":"巨蟹座","weight":62,"ageGroup":"18-24","uid":230,"birth_decade":2000,"login_name":"g60o358w6i6t","unit_height":"cm","name":"苏涛","user_level":1,"id":230,"unit_weight":"kg","age":19,"height":171},"Age":{"category_name":"xiaomiSu7","sku_num":1,"create_time":"1746574268000","os":"Android","sku_id":18,"price_sensitive":"高价商品","original_total_amount":29806.0,"keyword_type":"家庭与育儿","category1_id":"3","tm_name":"TCL","user_id":230,"total_amount":28886.1,"category1_name":"家用电器","sku_name":"TCL 75Q10 75英寸 QLED原色量子点电视 安桥音响 AI声控智慧屏 超薄全面屏 MEMC防抖 3+32GB 平板电视","id":1296,"keyword":"xiaomiSu7","order_id":753,"category3_id":"86","ts_ms":1747050794602,"category2_id":"16","time_period":"早晨","ts":1747004587863},"ageScore":{"tm_score":0.18,"price_sensitive_score":0.015,"time_period_score":0.01,"category_score":0.06,"os_score":0.08,"keyword_type_score":0.015}}
//        AgeEachScore.print();
        // 求和
        SingleOutputStreamOperator<JSONObject> TotalScore = AgeEachScore.map(new ScoreCalculator());
//        {"user":{"birthday":"2005-07-06","gender":"M","create_time":"1746528898000","zodiac_sign":"巨蟹座","weight":62,"ageGroup":"18-24","uid":230,"birth_decade":2000,"login_name":"g60o358w6i6t","unit_height":"cm","name":"苏涛","user_level":1,"id":230,"unit_weight":"kg","age":19,"height":171},"Age":{"category_name":"xiaomiSu7","sku_num":1,"create_time":"1746574268000","os":"Android","sku_id":24,"price_sensitive":"高价商品","original_total_amount":29806.0,"keyword_type":"家庭与育儿","category1_id":"14","tm_name":"金沙河","user_id":230,"total_amount":28886.1,"category1_name":"食品饮料、保健食品","sku_name":"金沙河面条 原味银丝挂面 龙须面 方便速食拉面 清汤面 900g","id":1292,"keyword":"xiaomiSu7","order_id":753,"category3_id":"803","ts_ms":1747050794602,"category2_id":"82","time_period":"早晨","ts":1747004587863},"ageScore":{"tm_score":0.02,"price_sensitive_score":0.015,"time_period_score":0.01,"category_score":0.03,"os_score":0.08,"TotalScore":0.170,"keyword_type_score":0.015}}
//        TotalScore.print();

        env.execute();
    }
    // 计算加权求和
    public static class ScoreCalculator implements MapFunction<JSONObject, JSONObject> {
        @Override
        public JSONObject map(JSONObject value)   {
            JSONObject ageScore = value.getJSONObject("ageScore");
            JSONObject user = value.getJSONObject("user");
            if (ageScore == null) return value;


            // 使用BigDecimal保证精度
            BigDecimal total = BigDecimal.ZERO;
            for (String key : ageScore.keySet()) {
                if ("TotalScore".equals(key)) continue;

                Object val = ageScore.get(key);
                if (val instanceof Number) {
                    // 通过字符串转换避免精度丢失
                    BigDecimal num = new BigDecimal(val.toString());
                    total = total.add(num);
                }
            }

            // 四舍五入保留3位小数
            total = total.setScale(3, RoundingMode.HALF_UP);
            double aDouble = total.doubleValue();

            if (aDouble >= 0.75){
                user.put("new_ageGroup", "18-24");
            } else if (aDouble >= 0.69) {
                user.put("new_ageGroup", "25-29");
            } else if (aDouble >= 0.585) {
                user.put("new_ageGroup", "30-34");
            } else if (aDouble >= 0.47) {
                user.put("new_ageGroup", "35-39");
            } else if (aDouble >= 0.365) {
                user.put("new_ageGroup", "40-49");
            } else if (aDouble >= 0.26) {
                user.put("new_ageGroup", "50+");
            }else {
                user.put("new_ageGroup", "数据无效");
            }

            // 更新 TotalScore
            ageScore.put("TotalScore", total);
            return value;
        }
    }
}
