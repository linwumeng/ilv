package com.vipxf.regulation.ilv.core.command;

import com.vipxf.regulation.ilv.core.Context;
import com.vipxf.regulation.ilv.core.StepConfig;
import org.springframework.util.StringUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class JournalCommand implements Command {
    private StepConfig config;

    private Map<String, String> varTable = new HashMap<>();

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
        long batch = 10000;

        boolean hasRemaining = true;
        try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFilePath));
             FileChannel outChannel = new FileOutputStream(config.getTarget(), true).getChannel()) { // Open the file in append mode

            ZipEntry entry = zipIn.getNextEntry();
            while (entry != null && !entry.isDirectory()) {
                String entryName = entry.getName();
                if (entryName.equals(source)) {

                    BufferedReader reader = new BufferedReader(new InputStreamReader(zipIn));
                    Stream<String> csvStream = reader.lines().limit(batch);
                    while (hasRemaining) {
                        System.out.println("Processing CSV file: " + entryName + ": position: " + position);
                        hasRemaining = false;
                        csvStream = new StreamBuilder().buildStream(csvStream, (List<Map<String, Object>>) config.getConfig().get("prepare"));
                        hasRemaining = write(csvStream, outChannel, hasRemaining);

                        position += batch;
                        csvStream = reader.lines().limit(batch);
                    }
                }
                zipIn.closeEntry();
                break;
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Processing CSV file done ");
    }

    private static boolean write(Stream<String> csvStream, FileChannel outChannel, boolean hasRemaining) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(csvStream.reduce("", (f, l ) -> f + l + "\n").getBytes(Charset.forName("GBK")));
        while (buffer.hasRemaining()) {
            outChannel.write(buffer);
            hasRemaining = true;
        }
        return hasRemaining;
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

}

