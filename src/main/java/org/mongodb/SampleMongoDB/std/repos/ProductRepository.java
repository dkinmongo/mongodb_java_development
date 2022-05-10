package org.mongodb.SampleMongoDB.std.repos;

import org.mongodb.SampleMongoDB.std.dto.Product;
import org.springframework.data.mongodb.repository.MongoRepository;
import java.util.List;

public interface ProductRepository
        extends MongoRepository <Product, String> {
    public Product findByName(String name);
    public List<Product> findByType(String type);
}
