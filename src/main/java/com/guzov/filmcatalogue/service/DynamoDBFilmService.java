package com.guzov.filmcatalogue.service;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.guzov.filmcatalogue.dto.FilmRequest;
import com.guzov.filmcatalogue.model.FilmInfo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DynamoDBFilmService {
    private String tableName;
    private DynamoDB dynamoDB;
    private static final String AND_DELIMITER = " and ";

    public DynamoDBFilmService(String tableName) {
        this.tableName = tableName;
        this.dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.standard().build());
    }

    public List<FilmInfo> getByRequest(FilmRequest request) {
        validateRequest(request);
        List<FilmInfo> infos = new ArrayList<>();
        Table table = dynamoDB.getTable(tableName);
        QuerySpec querySpec = getQuerySpec(request);
        try {
            ItemCollection<QueryOutcome> itemCollection = table.query(querySpec);
            Iterator<Item> itemIterator = itemCollection.iterator();
            ObjectMapper mapper = new ObjectMapper();
            while (itemIterator.hasNext()) {
                FilmInfo retrievedRecord = mapper.readValue(itemIterator.next().toJSON(), FilmInfo.class);
                infos.add(retrievedRecord);
            }
        } catch (AmazonDynamoDBException | JsonProcessingException e) {
            throw new RuntimeException("Error while querying records from DynamoDB", e);
        }
        return infos;
    }

    private void validateRequest(FilmRequest request) {
        if (request == null || request.getType() == null) {
            throw new IllegalArgumentException("Request must not be null and film type should be specified");
        }
    }

    private QuerySpec getQuerySpec(FilmRequest request) {
        QuerySpec querySpec = new QuerySpec();
        ValueMap valueMap = new ValueMap();
        if (request.getType() != null) {
            querySpec.withKeyConditionExpression("filmType = :v_tpe");
            valueMap.put(":v_tpe", request.getType());
        }
        if (request.getCountry() != null) {
            addFilter(querySpec, "country = :v_cntry");
            valueMap.put(":v_cntry", request.getCountry());
        }
        if (request.getDirector() != null) {
            addFilter(querySpec, "director = :v_drctr");
            valueMap.put(":v_drctr", request.getDirector());
        }
        if (request.getGenre() != null) {
            addFilter(querySpec, "genre = :v_gnre");
            valueMap.put(":v_gnre", request.getGenre());
        }
        if (request.getYearStart() != null && request.getYearEnd() != null) {
            addFilter(querySpec, "dateCreated between :v_yrstart and :v_yrend");
            valueMap.put(":v_yrstart", request.getYearStart());
            valueMap.put("v_yrend", request.getYearEnd());
        }
        querySpec.withValueMap(valueMap);
        return querySpec;
    }

    private static void addFilter(QuerySpec querySpec, String filter) {
        String filterExpression = querySpec.getFilterExpression();
        if (filterExpression != null && !filterExpression.isBlank()) {
            filterExpression = String.join(AND_DELIMITER, filterExpression, filter);
        } else {
            filterExpression = filter;
        }
        querySpec.withFilterExpression(filterExpression);
    }
}
