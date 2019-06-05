package cn.dubby.flink.sql.demo.stream;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author dubby
 * @date 2019/6/5 15:28
 */
public class RandomStringSource implements SourceFunction<String> {

    private static final String[] strings = {"daoxuan", "yangzheng", "dubby"};

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        for (int i = 0; i < 10000 * 10000; ++i) {
            int random = ThreadLocalRandom.current().nextInt(3);
            ctx.collect(strings[random]);
        }
    }

    @Override
    public void cancel() {

    }
}
