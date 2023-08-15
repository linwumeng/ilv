package com.vipxf.regulation.ilv.core.command;

import com.vipxf.regulation.ilv.core.Context;
import com.vipxf.regulation.ilv.core.StepConfig;
import org.springframework.util.StringUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class JournalCommand implements Command {
    private StepConfig config;

    private Map<String, String> varTable = new HashMap<>();;

    public JournalCommand(StepConfig stepConfig) {
        this.config = stepConfig;

        this.varTable.put("source", String.join(".", stepConfig.getSourceVarName()));
    }

    public Set<String> vars() {
        return new HashSet<>(varTable.values());
    }

    @Override
    public void execute(Context context) {
        String zipFilePath = context.source(); // 替换为您的zip文件路径
        String source = context.get(config.getSourceVarName());
        long position = 0;
        long batch = 1000;

        try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFilePath));
             FileChannel outChannel = new FileOutputStream(config.getTarget(), true).getChannel()) { // Open the file in append mode

            ZipEntry entry = zipIn.getNextEntry();
            while (entry != null && !entry.isDirectory()) {
                String entryName = entry.getName();
                if (entryName.equals(source)) {
                    System.out.println("Processing CSV file: " + entryName);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(zipIn));

                    Stream<String> csvStream = reader.lines().skip(position).limit(batch);
                    do {
                        csvStream = processCSVStream(csvStream, (List<Map<String, Object>>) config.getConfig().get("prepare"));

                        ByteBuffer buffer = ByteBuffer.wrap(csvStream.reduce("", (f, l) -> f+l).getBytes(StandardCharsets.UTF_8));
                        while (buffer.hasRemaining()) {
                            outChannel.write(buffer);
                        }

                        position += batch;
                        csvStream = reader.lines().skip(position).limit(batch);
                    } while (csvStream.count() > 0);
                }
                zipIn.closeEntry();
                break;
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private String resolveVarName(String name) {
        if (StringUtils.startsWithIgnoreCase(name, "$")) {
            return name.substring(1);
        }

        return name;
    }

    private Stream<String> streamLines(InputStream inputStream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        return reader.lines();
    }

    private Stream<String> processCSVStream(Stream<String> csvStream, List<Map<String, Object>> operations) {
        // 在这里对CSV文件的每个分片流进行处理
        return new StreamBuilder().buildStream(csvStream, operations);
    }

}

