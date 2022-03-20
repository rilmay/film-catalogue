package com.guzov.filmcatalogue.model;

import java.util.Objects;

public class FilmInfo {
    private Long id;
    private String dateCreated;
    private String country;
    private String director;
    private String genre;
    private String filmType;
    private String name;

    public FilmInfo() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDateCreated() {
        return dateCreated;
    }

    public void setDateCreated(String dateCreated) {
        this.dateCreated = dateCreated;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getDirector() {
        return director;
    }

    public void setDirector(String director) {
        this.director = director;
    }

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public String getFilmType() {
        return filmType;
    }

    public void setFilmType(String filmType) {
        this.filmType = filmType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "FilmInfo{" +
                "id=" + id +
                ", dateCreated='" + dateCreated + '\'' +
                ", country='" + country + '\'' +
                ", director='" + director + '\'' +
                ", genre='" + genre + '\'' +
                ", filmType='" + filmType + '\'' +
                ", name='" + name + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FilmInfo filmInfo = (FilmInfo) o;
        return Objects.equals(id, filmInfo.id)
                && Objects.equals(dateCreated, filmInfo.dateCreated)
                && Objects.equals(country, filmInfo.country)
                && Objects.equals(director, filmInfo.director)
                && Objects.equals(genre, filmInfo.genre)
                && Objects.equals(filmType, filmInfo.filmType)
                && Objects.equals(name, filmInfo.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, dateCreated, country, director, genre, filmType, name);
    }
}
