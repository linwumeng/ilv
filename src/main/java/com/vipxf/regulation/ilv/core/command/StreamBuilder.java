package com.vipxf.regulation.ilv.core.command;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamBuilder {
    public Stream<String> buildStream(Stream<String> inputStream, List<Map<String, Object>> operations) {

        // 构建操作链并应用于输入流
        Stream<String> outputStream = inputStream;
        for (Map<String, Object> operation : operations) {
            String operationName = (String) operation.get("name");
            switch (operationName) {
                case "select":
                    List<Integer> selectColumns = (List<Integer>) operation.get("columns");
                    outputStream = outputStream.map(line -> {
                        String[] fields = line.split(",");
                        List<String> selectedFields = selectColumns.stream()
                                .map(index -> fields[index])
                                .collect(Collectors.toList());
                        return String.join(",", selectedFields);
                    });
                    break;
                case "map":
                    int mapFromColumn = (int) operation.get("from");
                    String action = (String) operation.get("action");
                    List<Map<String, Object>> enumMappings = (List<Map<String, Object>>) operation.get("enum");
                    outputStream = outputStream.map(line -> {
                        String[] fields = line.split(",");
                        String value = fields[mapFromColumn];
                        String mappedValue = mapEnumValue(value, enumMappings);
                        if ("replace".equalsIgnoreCase(action)) {
                            fields[mapFromColumn] = mappedValue;
                        } else {
                            fields = addElement(fields, mappedValue);
                        }

                        return String.join(",", fields);
                    });
                    break;
                case "expression":
                    int expressionToColumn = (int) operation.get("to");
                    String expression = (String) operation.get("expression");
                    outputStream = outputStream.map(line -> {
                        String[] fields = line.split(",");
                        double result = evaluateExpression(expression, fields);
                        fields[expressionToColumn] = String.valueOf(result);
                        return String.join(",", fields);
                    });
                    break;
                case "groupBySum":
                    List<Integer> groupByColumns = (List<Integer>) operation.get("groupBy");
                    List<Integer> sumColumns = (List<Integer>) operation.get("sum");
                    outputStream = outputStream.collect(Collectors.groupingBy(line -> {
                        String[] fields = line.split(",");
                        List<String> groupByValues = groupByColumns.stream()
                                .map(index -> fields[index])
                                .collect(Collectors.toList());
                        return String.join(",", groupByValues);
                    })).values().stream().map(group -> {
                        String[] firstLineFields = group.get(0).split(",");
                        for (int sumColumn : sumColumns) {
                            double sum = group.stream()
                                    .mapToDouble(line -> Double.parseDouble(line.split(",")[sumColumn]))
                                    .sum();
                            firstLineFields[sumColumn] = String.valueOf(sum);
                        }
                        return String.join(",", firstLineFields);
                    });
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operation: " + operationName);
            }
        }

        return outputStream;
    }

    public static String[] addElement(String[] originalArray, String newElement) {
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

    private double evaluateExpression(String expression, String[] fields) {
        SpelExpressionParser parser = new SpelExpressionParser();
        Expression exp = parser.parseExpression(expression);
        StandardEvaluationContext evaluationContext = new StandardEvaluationContext();

        for (int i = 0; i < fields.length; i++) {
            evaluationContext.setVariable(String.valueOf(i), fields[i]);
        }

        return exp.getValue(evaluationContext, Double.class);
    }
}
