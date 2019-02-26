package mflix.api.daos;

import com.mongodb.client.MongoClient;
import mflix.config.MongoDBConfiguration;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SpringBootTest(classes = {MongoDBConfiguration.class})
@EnableConfigurationProperties
@EnableAutoConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class TextAndSubfieldTest extends TicketTest {

    private MovieDao dao;
    private String sortKey;
    @Autowired
    MongoClient mongoClient;

    @Value("${spring.mongodb.database}")
    String databaseName;

    @Before
    public void setup() {
        this.dao = new MovieDao(mongoClient, databaseName);
        this.sortKey = "tomatoes.viewer.numReviews";
    }

    @Test
    public void testTextSearch() {
        String keywords = "dress";
        int skip = 0;
        int limit = 20;

        Iterable<Document> cursor = dao.getMoviesByText(limit, skip, keywords);

        Document firstMovie = cursor.iterator().next();

        Assert.assertEquals(
                "Movie title does not match expected. Check your sort",
                "The Dress",
                firstMovie.getString("title"));

        int actualMoviesCount = 0;
        for (Document doc : cursor) {
            actualMoviesCount++;
        }

        Assert.assertEquals(
                "The expect number of movies does not match. Check your query filter",
                limit,
                actualMoviesCount);
    }

    @Test
    public void testTextSearchCount() {
        long expected = 326;
        String keywords = "dress";

        Assert.assertEquals(
                "Text search matched documents does not match. Check your query filter",
                expected,
                dao.getTextSearchCount(keywords));
    }

    @Test
    public void testSearchByCast() {
        List<Document> movies = dao.getMoviesByCast(sortKey, 10, 0, "Elon Musk");
        int expectedCount = 1;
        Assert.assertEquals("The expect number of documents does not match. Check your query filter",
                expectedCount, movies.size());

        Document movie = movies.get(0);
        Assert.assertEquals(
                "Expected title does not match. Check your query filter",
                "Racing Extinction", movie.getString("title"));
    }

    @Test
    public void testSearchMultipleCast() {
        ArrayList<String> cast = new ArrayList<>();
        cast.add("Elon Musk");
        cast.add("Robert Redford");
        cast.add("Julia Roberts");

        List<Document> cursor = dao.getMoviesByCast(sortKey, 33, 0, cast.toArray(new String[0]));

        Assert.assertEquals(
                "Number of movies expected does not match. Check your query filter", 33, cursor.size());

        Assert.assertEquals(
                "Total count of movies with cast does not match. Check your query filter",
                75,
                dao.getCastSearchCount(cast.toArray(new String[0])));
    }

    @Test
    public void testMultipleGenreSearch() {
        ArrayList<String> genres = new ArrayList<>();
        genres.add("Action");
        genres.add("Adventure");
        int limit = 25;
        int skip = 0;
        String[] garray = genres.toArray(new String[0]);
        List<Document> movies = dao.getMoviesByGenre(sortKey, limit, skip, garray);

        assertTrue("getMoviesByGenre should be returning documents", movies.size() > 0);
        assertEquals(8385, dao.getGenresSearchCount(garray));
    }

    @Test
    public void testGenreSearch() {
        List<Document> movies = dao.getMoviesByGenre(sortKey, 20, 0, "Action");

        assertTrue("getMoviesByGenre should be returning documents", movies.size() > 0);
        assertEquals("Number of total documents does not match expected. Check your dataset",
                5917, dao.getGenresSearchCount("Action"));
    }
}
