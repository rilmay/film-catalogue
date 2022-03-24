package com.guzov.filmcatalogue.dto;

import java.util.Objects;

public class FilmRequest {
    private String type;

    public FilmRequest() {
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FilmRequest that = (FilmRequest) o;
        return Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }

    @Override
    public String toString() {
        return "FilmRequest{" +
                "type='" + type + '\'' +
                '}';
    }
}
