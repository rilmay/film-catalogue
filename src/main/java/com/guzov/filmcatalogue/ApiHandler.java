package com.guzov.filmcatalogue;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.guzov.filmcatalogue.dto.FilmRequest;
import com.guzov.filmcatalogue.model.FilmInfo;
import com.guzov.filmcatalogue.service.DynamoDBFilmService;
import com.guzov.filmcatalogue.util.Constants;

import java.util.List;

public class ApiHandler implements RequestHandler<FilmRequest, String> {
    public String handleRequest(FilmRequest filmRequest, Context context) {
        DynamoDBFilmService dynamoDBFilmService = new DynamoDBFilmService(Constants.FILM_TABLE_NAME);
        List<FilmInfo> infos = dynamoDBFilmService.getByRequest(filmRequest);
        return infos.toString();
    }
}
