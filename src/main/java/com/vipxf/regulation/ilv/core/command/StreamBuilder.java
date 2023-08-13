package com.vipxf.regulation.ilv.core.command;

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
                        String mappedValue = mapValue(value, action, enumMappings);
                        fields[mapFromColumn] = mappedValue;
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

    private String mapValue(String value, String action, List<Map<String, Object>> enumMappings) {
        for (Map<String, Object> mapping : enumMappings) {
            String from = String.valueOf(mapping.get("from"));
            String to = String.valueOf(mapping.get("to"));
            if (action.equals("replace") && value.equals(from)) {
                return to;
            } else if (action.equals("append") && value.contains(from)) {
                return value + to;
            }
        }
        return value;
    }

    private double evaluateExpression(String expression, String[] fields) {
//            this.parser = new SpelExpressionParser();
//            this.expression = parser.parseExpression(expression);
//            this.evaluationContext = new StandardEvaluationContext();
//
//            evaluationContext.setVariable("0", item.getColumn(0));
//            evaluationContext.setVariable("1", item.getColumn(1));
//            evaluationContext.setVariable("2", item.getColumn(2));
//
//            YourOutputObject output = new YourOutputObject();
//            output.setColumn(3, expression.getValue(evaluationContext, Integer.class));
//            return output;
        return 0;
    }
}
