package com.vipxf.regulation.ilv.core.command;

import com.vipxf.regulation.ilv.core.Context;
import com.vipxf.regulation.ilv.core.StepConfig;
import org.apache.logging.log4j.util.Strings;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JoinCommand implements Command {
    private StepConfig config;

    public JoinCommand(StepConfig stepConfig) {
        this.config = stepConfig;
    }

    @Override
    public void execute(Context context) {
        System.out.println("Join files");
        List<Map<String, List<Integer>>> source = (List<Map<String, List<Integer>>>) config.getConfig().get("source");
        try {
            // Load the SQLite JDBC driver
            Class.forName("org.sqlite.JDBC");
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            String sourceKey = source.iterator().next().keySet().iterator().next() + ".target";
            File sourceFile = new File(context.resolveFile(sourceKey));
            // Connect to the database
            Connection connection = DriverManager.getConnection("jdbc:sqlite:" + sourceFile.getParentFile().getCanonicalPath() + File.separator + "tmp.db");
            // Disable auto-commit mode
            connection.setAutoCommit(false);

            Statement statement = connection.createStatement();
            // Use PRAGMA to optimize SQLite
//            statement.execute("PRAGMA synchronous=OFF");
//            statement.execute("PRAGMA journal_mode=WAL");
//            statement.close();

            List<Map<String, Object>> sources = parseSource(context, source);
            for (Map<String, Object> s : sources) {
                int totalColumns = Integer.parseInt(s.get("total").toString());
                String table = s.get("table").toString();;

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(context.resolveFile(s.get("key").toString()))))) {
                    String createTable = createTableStatement(totalColumns, (List<Integer>) s.get("keys"), table);

                    statement = connection.createStatement();
                    statement.execute(createTable);
                    statement.close();

                    executeBatch(connection, table, totalColumns, reader);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }


            statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("SELECT * FROM KHYE5_csv as k5 full join JJFSE_csv as jj on k5.id1=jj.id1 and k5.id2=jj.id2 full join KHYE6_csv as k6 on jj.id1=k6.id1 and jj.id2=k6.id2");
            int i=0;
            while (rs.next()) {
                i++;
                System.out.println(String.join(" ",rs.getString(1),rs.getString(2),rs.getString(3),rs.getString(5),rs.getString(6),rs.getString(7),rs.getString(8),rs.getString(9)));
            }
            System.out.println(i);
            rs.close();
            connection.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }

    private static List<Map<String, Object>> parseSource(Context context, List<Map<String, List<Integer>>> source) throws IOException {
        List<Map<String,Object>> tables = new ArrayList<>(3);
        for (Map<String, List<Integer>> s : source) {
            String key = s.keySet().iterator().next() + ".target";
            int totalColumns = s.values().iterator().next().get(0);
            File file = new File(context.resolveFile(key));
            String table = file.getCanonicalFile().getName().replace('.', '_').replace('-', '_');
            List<Integer> keys = new ArrayList<>();
            s.values().iterator().next().listIterator(1).forEachRemaining(line -> {
                keys.add(line);
            });
            Map<String, Object> map = new HashMap<>();
            map.put("table",table);
            map.put("total", totalColumns);
            map.put("keys", keys);
            map.put("key",key);

            tables.add(map);
        }

        return tables;
    }

    private static void executeBatch(Connection connection, String table, int totalColumns, BufferedReader reader) throws SQLException {
        String placeholder = createPlaceholder(totalColumns);
        // Use a PreparedStatement for batch inserts
        PreparedStatement pstmt = connection.prepareStatement("INSERT INTO " + table + " VALUES (" + placeholder + ")");
        for (int i = 0; i < 100000; i++) {
            int k = i;
            reader.lines().map(line -> line.split("\\|")).forEach(line -> {
                try {
                    for (int j = 0; j < line.length; j++) {
                        pstmt.setString(j + 1, line[j]);
                    }
                    pstmt.addBatch();
                    if (k % 1000 == 0) {
                        pstmt.executeBatch(); // Execute every 1000 items.
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        pstmt.executeBatch(); // insert remaining records

        // Commit the transaction
        connection.commit();

        // Close the connection
        pstmt.close();
    }

    private static Connection connect(File file, String table, String createTable) throws SQLException, IOException {
        // Connect to the database
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + file.getParentFile().getCanonicalPath() + File.separator + table + ".db");

        Statement statement = connection.createStatement();
        statement.execute(createTable);
        // Use PRAGMA to optimize SQLite
        statement.execute("PRAGMA synchronous=OFF");
        statement.execute("PRAGMA journal_mode=WAL");
        statement.close();
        return connection;
    }

    private static String createPlaceholder(int totalCclumns) {
        String[] marks = new String[totalCclumns];
        for (int i = 0; i < totalCclumns; i++) {
            marks[i] = "?";
        }
        String placeholder = String.join(",", marks);
        return placeholder;
    }

    private static String createTableStatement(int totalCclumns, List<Integer> keys, String table) {
        List<String> keyFields = keys.stream().map(i -> String.format("id%d", i)).collect(Collectors.toList());
        List<String> fields = createFieldName(totalCclumns, keys);
        String createTable = "CREATE TABLE IF NOT EXISTS " + table + " ("
                + Strings.join(fields, ',')
                + ","
                + " PRIMARY KEY(" + Strings.join(keyFields, ',') + ")"
                + ")";

        return createTable;
    }

    private static List<String> createFieldName(int totalCclumns, List<Integer> keys) {
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < totalCclumns; i++) {
            String field = String.format("%d TEXT", i + 1);
            if (keys.contains(i + 1)) {
                field = "id" + field;
            } else {
                field = "field" + field;
            }

            fields.add(field);
        }
        return fields;
    }
}
