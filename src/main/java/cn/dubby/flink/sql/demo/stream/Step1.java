package cn.dubby.flink.sql.demo.stream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.InMemoryExternalCatalog;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;

/**
 * @author dubby
 * @date 2019/3/7 21:37
 */
public class Step1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(environment);

        // 创建一个 source
        TypeInformation[] sourceFieldTypes = {Types.INT, Types.STRING};
        TableSource<org.apache.flink.types.Row> csvSource = new CsvTableSource("D:\\JavaRepo\\flink-sql-demo\\src\\main\\resources\\source.csv", new String[]{"id", "name"}, sourceFieldTypes);
        tableEnvironment.registerTableSource("CsvSourceTable", csvSource);

        //  创建一个 sink
        TableSink<org.apache.flink.types.Row> csvSink = new CsvTableSink("D:\\JavaRepo\\flink-sql-demo\\src\\main\\resources\\sink", ",", 2, FileSystem.WriteMode.OVERWRITE);
        String[] fieldNames = {"id", "greeting"};
        TypeInformation[] sinkFieldTypes = {Types.INT, Types.STRING};
        tableEnvironment.registerTableSink("CsvSinkTable", fieldNames, sinkFieldTypes, csvSink);

        // 创建一个外部存储
        ExternalCatalog catalog = new InMemoryExternalCatalog("memory");
        tableEnvironment.registerExternalCatalog("InMemCatalog", catalog);

        // 创建一个 view
        Table table = tableEnvironment.scan("CsvSourceTable").select("id, 'Hello, ' + name as greeting");
        tableEnvironment.registerTable("GreetingView", table);

        Table greeting = tableEnvironment.sqlQuery(
                "SELECT id, greeting " +
                        "FROM GreetingView "
        );

        greeting.writeToSink(csvSink);

        environment.execute();
    }

}
