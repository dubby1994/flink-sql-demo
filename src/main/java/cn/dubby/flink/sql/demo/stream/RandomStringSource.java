package cn.dubby.flink.sql.demo.stream;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author dubby
 * @date 2019/6/5 15:28
 */
public class RandomStringSource implements SourceFunction<String> {

    private static final String[] strings = {"a", "b", "c", "d", "e", "f"};

    private volatile boolean cancel = false;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (!cancel) {
            int random = ThreadLocalRandom.current().nextInt(strings.length);
            ctx.collect(strings[random]);
        }
    }

    @Override
    public void cancel() {
        cancel = true;
    }
}
