package org.mongodb.SampleMongoDB.std.dto;

import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;

import java.util.ArrayList;

@Data
public class Product {
    @Id
    public ObjectId _id;
    public String name;
    public String type;
    public ArrayList tags;
    public Release release;
    public ArrayList<Review> review;
    public Product() {
    }

    public Product(String name, String type,
                   ArrayList tags,
                   Release release,
                   ArrayList<Review> review) {
        this.name = name;
        this.type = type;
        this.tags = tags;
        this.release = release;
        this.review = review;
    }
}
