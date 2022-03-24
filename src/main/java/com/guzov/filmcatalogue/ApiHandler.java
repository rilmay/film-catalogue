package com.guzov.filmcatalogue;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.guzov.filmcatalogue.dto.FilmRequest;
import com.guzov.filmcatalogue.model.FilmInfo;
import com.guzov.filmcatalogue.service.DynamoDBFilmService;
import com.guzov.filmcatalogue.service.DynamoDBFilmServiceException;
import com.guzov.filmcatalogue.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ApiHandler implements RequestHandler<FilmRequest, String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApiHandler.class);

    public String handleRequest(FilmRequest filmRequest, Context context) {
        LOGGER.info("Start");
        try {
            DynamoDBFilmService dynamoDBFilmService = new DynamoDBFilmService(Constants.FILM_TABLE_NAME);
            LOGGER.info("DynamoDB service initialized");
            List<FilmInfo> filmInfoList = dynamoDBFilmService.getByType(filmRequest.getType());
            LOGGER.info("Records queried {}", filmInfoList);
        } catch (DynamoDBFilmServiceException e) {
            return "Failed while loading records";
        }
        return "Success";
    }
}
