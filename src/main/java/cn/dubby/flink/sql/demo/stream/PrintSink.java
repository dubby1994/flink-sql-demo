package cn.dubby.flink.sql.demo.stream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dubby
 * @date 2019/6/5 15:29
 */
public class PrintSink implements AppendStreamTableSink<Row> {

    private static final Logger logger = LoggerFactory.getLogger(PrintSink.class);

    private String[] fieldNames = {"word", "c", "ts"};

    private TypeInformation[] sinkFieldTypes = {Types.STRING, Types.LONG, Types.SQL_TIMESTAMP};

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        dataStream
                .map(row -> "(" + row.getField(2) + ")" + row.getField(0) + "\t" + row.getField(1) + "\t")
                .addSink(new SinkFunction<String>() {
                    @Override
                    public void invoke(String value, SinkFunction.Context context) throws Exception {
                        logger.info(value);
                    }
                });


    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return new RowTypeInfo(sinkFieldTypes);
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return sinkFieldTypes;
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return null;
    }
}
