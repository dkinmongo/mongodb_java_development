package org.mongodb.SampleMongoDB.config;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.mapping.model.SnakeCaseFieldNamingStrategy;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import java.util.concurrent.TimeUnit;

@Configuration
public class MongoConfiguration {
    @Autowired
    private Environment env;

    @Autowired
    public MongoClient mongoClient;

    @Autowired
    public MongoDatabaseFactory mongoDatabaseFactory;

    @Bean
    public MongoTransactionManager transactionManager() {
        return new MongoTransactionManager(mongoDatabaseFactory);
    }

    @Bean
    public MongoTemplate mongoTemplate() {
        MappingMongoConverter converter = mappingMongoConverter();
        converter.setTypeMapper(new DefaultMongoTypeMapper(null));
        converter.afterPropertiesSet();
        return new MongoTemplate(mongoDatabaseFactory, converter);
    }

    private MappingMongoConverter mappingMongoConverter() {
        DbRefResolver dbRefResolver = new DefaultDbRefResolver(mongoDatabaseFactory);
        MongoMappingContext mongoMappingContext = new MongoMappingContext();
        mongoMappingContext.setFieldNamingStrategy(new SnakeCaseFieldNamingStrategy());
        MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, mongoMappingContext);

        return converter;
    }

    protected String getDatabaseName() {
        return env.getProperty("spring.data.mongodb.database");
    }

    @Bean
    public MongoClient mongoClient() {
        ConnectionString connectionString = new ConnectionString(
                env.getProperty("spring.data.mongodb.uri")
        );
        MongoClientSettings mongoClientSettings =
                MongoClientSettings.builder()
                        .applyConnectionString(connectionString)
                        .build();
        return MongoClients.create(mongoClientSettings);
    }
}
