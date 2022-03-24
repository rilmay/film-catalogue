package com.guzov.filmcatalogue.service;

public class DynamoDBFilmServiceException extends RuntimeException{
    public DynamoDBFilmServiceException() {
    }

    public DynamoDBFilmServiceException(String message) {
        super(message);
    }

    public DynamoDBFilmServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    public DynamoDBFilmServiceException(Throwable cause) {
        super(cause);
    }
}
