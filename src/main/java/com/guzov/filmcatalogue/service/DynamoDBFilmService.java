package com.guzov.filmcatalogue.service;

import com.guzov.filmcatalogue.model.FilmInfo;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamoDBFilmService {
    private final String tableName;
    private final DynamoDbClient dynamoDB;

    public DynamoDBFilmService(String tableName) {
        this.tableName = tableName;
        dynamoDB = DynamoDbClient
                .builder()
                .region(Region.US_EAST_2)
                .build();
    }

    public List<FilmInfo> getByType(String type) {
        Map<String, AttributeValue> attrValues = new HashMap<>();
        attrValues.put(":v_tpe", AttributeValue.builder().s(type).build());
        QueryRequest queryReq = QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("filmType = :v_tpe")
                .expressionAttributeValues(attrValues)
                .build();
        try {
            QueryResponse response = dynamoDB.query(queryReq);
            return getRecords(response.items());
        } catch (DynamoDbException e) {
            throw new DynamoDBFilmServiceException("Error while querying data", e);
        }
    }

    private List<FilmInfo> getRecords(List<Map<String, AttributeValue>> items) {
        List<FilmInfo> filmInfoList = new ArrayList<>();
        for (Map<String, AttributeValue> item : items) {
            FilmInfo filmInfo = new FilmInfo();
            filmInfo.setFilmType(item.get("filmType").s());
            filmInfo.setCountry(item.get("country").s());
            filmInfo.setDateCreated(item.get("dateCreated").s());
            filmInfo.setDirector(item.get("director").s());
            filmInfo.setName(item.get("name").s());
            filmInfo.setGenre(item.get("genre").s());
            filmInfo.setId(Long.parseLong(item.get("id").n()));
            filmInfoList.add(filmInfo);
        }
        return filmInfoList;
    }
}
