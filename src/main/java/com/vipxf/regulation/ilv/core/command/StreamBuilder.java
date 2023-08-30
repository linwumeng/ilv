package com.vipxf.regulation.ilv.core.command;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamBuilder {

    public static final String FIELD_DELIMITOR = "\\|";

    public Stream<String> buildStream(Stream<String> inputStream, List<Map<String, Object>> operations) {
        // 构建操作链并应用于输入流
        Stream<String> outputStream = inputStream;
        for (Map<String, Object> operation : operations) {
            String operationName = (String) operation.get("name");
            switch (operationName) {
                case "select":
                    List<Integer> selectColumns = (List<Integer>) operation.get("columns");
                    outputStream = outputStream.filter(line -> StringUtils.hasLength(line)).map(line -> {
                        String[] fields = line.split(FIELD_DELIMITOR);
                        List<String> selectedFields = selectColumns.stream()
                                .map(index -> fields[index - 1])
                                .collect(Collectors.toList());
                        return String.join("|", selectedFields);
                    });
                    break;
                case "map":
                    int mapFromColumn = (int) operation.get("from");
                    String action = (String) operation.get("action");
                    List<Map<String, Object>> enumMappings = (List<Map<String, Object>>) operation.get("enum");
                    outputStream = outputStream.map(line -> {
                        String[] fields = line.split(FIELD_DELIMITOR);
                        String value = fields[mapFromColumn - 1];
                        String mappedValue = mapEnumValue(value, enumMappings);
                        if ("replace".equalsIgnoreCase(action)) {
                            fields[mapFromColumn - 1] = mappedValue;
                        } else {
                            fields = append(fields, mappedValue);
                        }

                        return String.join("|", fields);
                    });
                    break;
                case "expression":
                    int expressionToColumn = (int) operation.get("to");
                    String expression = (String) operation.get("expression");
                    String elExpr = elExpr(expression);
                    outputStream = outputStream.map(line -> {
                        String[] fields = line.split(FIELD_DELIMITOR);
                        BigDecimal result = evaluateExpression(elExpr, fields);
                        fields[expressionToColumn - 1] = String.valueOf(result);
                        return String.join("|", fields);
                    });
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operation: " + operationName);
            }
        }

        return outputStream;
    }

    private static String[] append(String[] originalArray, String newElement) {
        int currentLength = originalArray.length;
        String[] newArray = new String[currentLength + 1];
        System.arraycopy(originalArray, 0, newArray, 0, currentLength);
        newArray[currentLength] = newElement;
        return newArray;
    }

    private String mapEnumValue(String value, List<Map<String, Object>> enumMappings) {
        for (Map<String, Object> mapping : enumMappings) {
            String from = String.valueOf(mapping.get("from"));
            String to = String.valueOf(mapping.get("to"));
            if (value.equals(from)) {
                return to;
            }
        }
        return "0";
    }

    @Data
    @AllArgsConstructor
    public static class RootObject {
        private String[] array;
    }

    private BigDecimal evaluateExpression(String expression, String[] fields) {
        SpelExpressionParser parser = new SpelExpressionParser();
        Expression exp = parser.parseExpression(expression);

        return exp.getValue(new RootObject(fields), BigDecimal.class);
    }

    private String elExpr(String customExpression) {
        Pattern pattern = Pattern.compile("@(\\d+)");
        Matcher matcher = pattern.matcher(customExpression);
        StringBuffer sb = new StringBuffer();

        while (matcher.find()) {
            matcher.appendReplacement(sb, "new java.math.BigDecimal(array[" + (Integer.parseInt(matcher.group(1)) - 1) + "])");
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
}
