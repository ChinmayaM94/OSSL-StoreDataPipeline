package com.safeway.ossl.storepipeline.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/**
 * The "grocerystores" collection is written by a different, external
 * service ({@code com.safeway.sloc.models.Store}) whose documents carry a
 * "_class" field pointing to a Java type that does not exist on this
 * project's classpath. Spring Data MongoDB's default converter uses that
 * "_class" hint to decide what Java type to instantiate on read, which can
 * silently interfere with mapping an unrelated/unresolvable hint onto our
 * own {@link com.safeway.ossl.storepipeline.models.GroceryStoreDocument}.
 * <p>
 * Registering a {@link MappingMongoConverter} with its type mapper disabled
 * ({@code new DefaultMongoTypeMapper(null)}) forces every read to use
 * exactly the Java type requested by the repository/query, ignoring any
 * "_class" value present in the stored document.
 */
@Configuration
public class MongoConverterConfig {

    @Bean
    public MappingMongoConverter mappingMongoConverter(MongoDatabaseFactory factory,
                                                        MongoMappingContext context,
                                                        MongoCustomConversions conversions) {
        DefaultDbRefResolver dbRefResolver = new DefaultDbRefResolver(factory);
        MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, context);
        converter.setCustomConversions(conversions);
        converter.setTypeMapper(new DefaultMongoTypeMapper(null)); // null disables _class-based type hinting
        converter.afterPropertiesSet();
        return converter;
    }
}

