package com.xy.dwd.app;

import com.xy.constant.Constant;
import com.xy.util.SQLUtil;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.jiao.dwd.app.DwdInteractionCommentInfo
 * @Author xinyi.jiao
 * @Date 2025/4/11 10:32
 * @description: 加购事实表
 */
public class DwdInteractionCommentInfo{
    @SneakyThrows
    public static void main(String[] args)  {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
//
        tenv.executeSql("" +
                "CREATE TABLE db (\n" +
                "  after Map<String,String>,\n" +
                "  source  Map<String,String>,\n" +
                "  op  String,\n" +
                "  ts_ms  bigint,\n" +
                "  before MAP<string,string> ,\n" +
                "  proc_time  AS proctime()\n "+
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'xinyi_jiao_yw',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
//

//        Table table = tenv.sqlQuery("select * from db ");
//        tenv.toChangelogStream(table).print();
//// TODO 关联数据 过滤 评论数据
         Table table1 = tenv.sqlQuery("select " +
                "after['id'] as id," +
                "after['user_id'] as user_id," +
                "after['sku_id'] as sku_id," +
                "after['appraise'] as appraise," +
                "after['comment_txt'] as comment_txt," +
                "ts_ms as ts," +
                "proc_time " +
                "from db where source['table'] = 'comment_info'");
//        tenv.toChangelogStream(table1).print("过滤===>");
////
        tenv.createTemporaryView("comment_info",table1);
//// TODO 关联数据
        tenv.executeSql("CREATE TABLE hbase (\n" +
                " dic_code String,\n" +
                " info ROW<dic_name String>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'ns_xinyi_jiao:dim_base_dic',\n" +
                " 'zookeeper.quorum' = 'cdh01,cdh02,cdh03:2181'\n" +
                ");");
//
//        Table table2 = tenv.sqlQuery("select * from hbase");
//        tenv.toChangelogStream(table2).print();
////
        Table table3 = tenv.sqlQuery("SELECT  " +
                " id,user_id,sku_id,appraise,dic.dic_name as appraise_name,comment_txt,ts \n" +
                "FROM comment_info AS c \n" +
                "  left join hbase as dic \n" +
                "    ON c.appraise = dic.dic_code;");
//        table3.execute().print();
////
////
        tenv.executeSql("CREATE TABLE "+ Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO+" (\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    appraise string,\n" +
                "    appraise_name string,\n" +
                "    comment_txt string,\n" +
                "    ts bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") " + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        // 写入
        table3.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
//        Table table4 = tenv.sqlQuery("select * from   dwd_interaction_xinyi_jiao_info ");
//        tenv.toChangelogStream(table4).print();
        env.execute();
    }
}
