package neo4j;

import neo4j.service.Neo4jService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.File;

@SpringBootApplication
public class App {

    @Value("${neo4j.uri}")
    private String neo4jUri;
    @Value("${neo4j.username}")
    private String neo4jUserName;
    @Value("${neo4j.password}")
    private String neo4jPassword;

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @Bean
    public Session session(Driver driver) {
        return driver.session();
    }

    @Bean
    public Driver driver() {
        return GraphDatabase.driver(neo4jUri, AuthTokens.basic(neo4jUserName, neo4jPassword));
    }

    @Bean
    public CommandLineRunner start(Neo4jService neo4jService) {

        return args -> {
            File file = new File(args[0]);

            LineIterator lineIterator = FileUtils.lineIterator(file);
            // skipping header
            lineIterator.nextLine();

            while (lineIterator.hasNext()) {
                String line = lineIterator.nextLine();
                neo4jService.ingest(line);
            }
        };
    }
}
