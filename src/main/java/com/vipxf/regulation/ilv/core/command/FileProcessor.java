package com.vipxf.regulation.ilv.core.command;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class FileProcessor {
    private String zipFilePath;
    private String entryName;
    private boolean hasRemaining = true;
    public FileProcessor(String zipFilePath, String entryName) {
        this.zipFilePath = zipFilePath;
        this.entryName = entryName;
    }

    public boolean process(String output, long position, long batch, List<Map<String, Object>> operations) {
        try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFilePath));
             FileChannel outChannel = new FileOutputStream(output, true).getChannel()) { // Open the file in append mode

            ZipEntry entry = zipIn.getNextEntry();
            while (entry != null && !entry.isDirectory()) {
                if (entryName.equals(entry.getName())) {

                    BufferedReader reader = new BufferedReader(new InputStreamReader(zipIn));
                    Stream<String> csvStream = reader.lines().limit(batch);
                    while (hasRemaining) {
                        System.out.println("Processing CSV file: " + entryName + ": position: " + position);
                        hasRemaining = false;
                        csvStream = new StreamBuilder().buildStream(csvStream, operations);
                        hasRemaining = write(csvStream, outChannel, hasRemaining);

                        position += batch;
                        csvStream = reader.lines().limit(batch);
                    }
                    zipIn.closeEntry();
                    break;
                }
                entry = zipIn.getNextEntry();
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    private static boolean write(Stream<String> csvStream, FileChannel outChannel, boolean hasRemaining) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(csvStream.reduce("", (f, l ) -> f + l + "\n").getBytes(Charset.forName("GBK")));
        while (buffer.hasRemaining()) {
            outChannel.write(buffer);
            hasRemaining = true;
        }
        return hasRemaining;
    }
}
