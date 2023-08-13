package com.vipxf.regulation.ilv.core.command;

import com.vipxf.regulation.ilv.core.Context;
import com.vipxf.regulation.ilv.core.StepConfig;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class JournalCommand implements Command {
    private StepConfig config;

    public JournalCommand(StepConfig stepConfig) {
        this.config = stepConfig;
    }

    @Override
    public void execute(Context context) {
        String zipFilePath = "path/to/your/archive.zip"; // 替换为您的zip文件路径

        try (FileInputStream fis = new FileInputStream(zipFilePath);
             ZipInputStream zis = new ZipInputStream(fis)) {

            ZipEntry entry = zis.getNextEntry();
            while (entry != null) {
                String entryName = entry.getName();
                if (entryName.endsWith(".csv")) {
                    System.out.println("Processing CSV file: " + entryName);
                    Stream<String> csvStream = processCSVFile(zis);
                    processCSVStream(csvStream, (List<Map<String, Object>>) config.getConfig().get("prepare"));
                }
                zis.closeEntry();
                entry = zis.getNextEntry();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Stream<String> processCSVFile(InputStream inputStream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        return reader.lines().limit(100);
    }

    private void processCSVStream(Stream<String> csvStream, List<Map<String, Object>> operations) {
        // 在这里对CSV文件的每个分片流进行处理
        new StreamBuilder().buildStream(csvStream, operations);
    }

}

