package com.vipxf.regulation.ilv.core.command;

import com.vipxf.regulation.ilv.core.Context;
import com.vipxf.regulation.ilv.core.StepConfig;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class JoinCommand implements Command{
    private StepConfig config;
    public JoinCommand(StepConfig stepConfig) {
        this.config = stepConfig;
    }

    @Override
    public void execute(Context context) {
        System.out.println("Join files");
        List<Map<String, List<String>>> source = (List<Map<String, List<String>>>) config.getConfig().get("source");
        for (Map<String, List<String>> s : source) {

        }
//        try (InputStream left = new FileInputStream(leftFile);
//             InputStream middle = new FileInputStream(middleFile);
//             InputStream right = new FileInputStream(rightFile);
//             FileChannel outChannel = new FileOutputStream(output, true).getChannel()) { // Open the file in append mode
//
//            BufferedReader leftReader = new BufferedReader(new InputStreamReader(left));
//            BufferedReader middleReader = new BufferedReader(new InputStreamReader(middle));
//            BufferedReader rightReader = new BufferedReader(new InputStreamReader(right));
//
//            Stream<String> csvStream = reader.lines().limit(batch);
//            while (hasRemaining) {
//                System.out.println("Processing CSV file: " + entryName + ": position: " + position);
//                hasRemaining = false;
//                csvStream = new StreamBuilder().buildStream(csvStream, operations);
//                hasRemaining = write(csvStream, outChannel, hasRemaining);
//
//                position += batch;
//                csvStream = reader.lines().limit(batch);
//            }
//        } catch (FileNotFoundException e) {
//            throw new RuntimeException(e);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }
}
