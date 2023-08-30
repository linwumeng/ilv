package com.vipxf.regulation.ilv.core.command;

import com.vipxf.regulation.ilv.core.Context;
import com.vipxf.regulation.ilv.core.StepConfig;
import org.apache.tomcat.util.buf.HexUtils;
import org.springframework.util.StringUtils;

import java.io.*;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.vipxf.regulation.ilv.core.command.StreamBuilder.FIELD_DELIMITOR;

public class ReduceCommand implements Command {
    private StepConfig config;

    static private char[] DIGITS_LOWER = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    static class Holder {
        private boolean hasRemaining = true;
    }
    public ReduceCommand(StepConfig stepConfig) {
        this.config = stepConfig;
    }

    @Override
    public void execute(Context context) {
        String workDir = String.join(File.separator, context.workdir(), randomString());
        String source = context.resolveFile(config.getSourceVarName());
        String target = config.getTarget(context);

        File wd = new File(workDir);
        if (!wd.exists()) {
            wd.mkdirs();
        }

        long batch = 10000;
        long position = 0;
        Holder h = new Holder();
        List<Integer> groupByColumns = (List<Integer>) config.getConfig().get("groupBy");
        List<Integer> sumColumns = (List<Integer>) config.getConfig().get("sum");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(source)))) {
            Stream<String> csvStream = reader.lines().limit(batch);
            while (h.hasRemaining) {
                System.out.println("Processing CSV file: " + source + ": position: " + position);
                h.hasRemaining = false;

                csvStream.parallel().collect(Collectors.groupingBy(line -> {
                    String[] fields = line.split(FIELD_DELIMITOR);
                    List<String> groupByValues = groupByColumns.stream()
                            .map(index -> fields[index - 1])
                            .collect(Collectors.toList());
                    return Integer.toHexString(Math.abs(String.join("", groupByValues).hashCode()) % 100);
                })).entrySet().forEach(e -> {
                    try (FileChannel outChannel = new FileOutputStream(String.join(File.separator, workDir, e.getKey()), true).getChannel()) {
                        ByteBuffer buffer = ByteBuffer.wrap(e.getValue().stream().reduce("", (f, l) -> f + l + "\n").getBytes(Charset.forName("GBK")));
                        outChannel.write(buffer);
                        h.hasRemaining = true;
                    } catch (FileNotFoundException fileNotFoundException) {
                        fileNotFoundException.printStackTrace();
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                });

                position += batch;
                csvStream = reader.lines().limit(batch);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try (FileChannel outChannel = new FileOutputStream(target, true).getChannel()) {
            Arrays.stream(wd.listFiles()).parallel().map(f -> {
                try {
                    System.out.println("process " + f.getCanonicalPath());
                    return new BufferedReader(new InputStreamReader(new FileInputStream(f))).lines();
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e.getMessage(), e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).map(lines ->
                    lines.collect(Collectors.groupingBy(line -> {
                        String[] fields = line.split(FIELD_DELIMITOR);
                        List<String> groupByValues = groupByColumns.stream()
                                .map(index -> fields[index - 1])
                                .collect(Collectors.toList());
                        return String.join("|", groupByValues);
                    })).values().stream().map(group -> {
                        String[] firstLineFields = group.get(0).split(FIELD_DELIMITOR);
                        for (int sumColumn : sumColumns) {
                            BigDecimal sum = group.stream()
                                    .map(line -> new BigDecimal(line.split(FIELD_DELIMITOR)[sumColumn - 1]))
                                    .reduce(new BigDecimal(0), (m,n) -> m.add(n));
                            firstLineFields[sumColumn - 1] = String.valueOf(sum);
                        }
                        return String.join("|", firstLineFields);
                    })
            ).forEach(l -> {
                try {
                    ByteBuffer buffer = ByteBuffer.wrap((l.reduce("", (m, n) -> m + n + "\n")).getBytes(Charset.forName("GBK")));
                    outChannel.write(buffer);
                }catch (IOException ioException) {
                    throw new RuntimeException(ioException.getMessage(), ioException);
                }
            });
            System.out.println("done");
        } catch (FileNotFoundException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    private String randomString() {
        Random r = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < 13; i++) {
            sb.append(DIGITS_LOWER[r.nextInt(16)]);
        }

        return sb.toString();
    }
}
