package cn.dubby.flink.sql.demo.stream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;

/**
 * @author dubby
 * @date 2019/3/7 21:37
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(environment);

        // 创建一个 source
        DataStream<String> ds = environment.addSource(new RandomStringSource());
        tableEnv.registerDataStream("word_source", ds, "word, proctime.proctime");

        // 创建一个 view
        Table countView = tableEnv.sqlQuery(
                "SELECT word, COUNT(1) as c, TUMBLE_END(proctime, INTERVAL '10' SECOND) as ts " +
                        "FROM word_source " +
                        "GROUP BY TUMBLE(proctime, INTERVAL '10' SECOND), word"
        );
        tableEnv.registerTable("count_view", countView);

        //  创建一个 sink
        TableSink<org.apache.flink.types.Row> printSink = new PrintSink();
        tableEnv.registerTableSink("count_sink", printSink);

        //输出到 sink
        tableEnv.sqlUpdate("INSERT INTO count_sink SELECT word, c, ts FROM count_view");

        environment.execute();
    }

}
