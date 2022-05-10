package org.mongodb.SampleMongoDB.std;

import com.mongodb.client.*;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import io.swagger.annotations.Api;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Updates.*;
import static java.util.Arrays.asList;

import java.util.function.Consumer;


@RestController
@Api(value = "TestController")
public class TestController {
    private static final Random rand = new Random();

    private static Logger logger = LoggerFactory.getLogger(TestController.class);

    @Autowired
    MongoClient mongoClient;

    @Autowired
    private MongoTemplate mongoTemplate;

    private static Document generateNewGrade(double studentId, double classId) {
        List<Document> scores = asList(new Document("type", "exam").append("score", rand.nextDouble() * 100),
                new Document("type", "quiz").append("score", rand.nextDouble() * 100),
                new Document("type", "homework").append("score", rand.nextDouble() * 100),
                new Document("type", "homework").append("score", rand.nextDouble() * 100));
        return new Document("_id", new ObjectId()).append("student_id", studentId)
                .append("class_id", classId)
                .append("scores", scores);
    }

    @RequestMapping(value = "/api/v1/insertone",
            consumes = "application/json",
            produces = "application/json",
            method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<String> insertOne(
            @RequestHeader HttpHeaders headers,
            @RequestBody Document bodyDoc) {

        ResponseEntity<String> responseEntity = null;

        try {
            if (logger.isDebugEnabled()) {
                logger.debug("========================================");
                logger.debug("HttpHeaders     : {}", headers.toString());
                logger.debug("RequestBody     : {}", bodyDoc.toString());
            }

            Document insertOneDoc = new Document(bodyDoc);

            // TODO : Controller - Service - DAO
            MongoDatabase testDB = mongoClient.getDatabase("test");
            MongoCollection<Document> sampleCollection = testDB.getCollection("sample");

            // InsertOne
            InsertOneResult insertOneResult = sampleCollection.insertOne(generateNewGrade(10000d, 1d));

            // Business 정의에 따라 error enum type 를 정의해서 front end 와 규약에 맞게 처리 바랍니다.
            if (insertOneResult.wasAcknowledged())
                responseEntity = new ResponseEntity<>("Insert Ok", HttpStatus.OK);
            else
                responseEntity = new ResponseEntity<>("Insert Fail", HttpStatus.NO_CONTENT);

        } catch (Exception e) {
            // Exception 에 따라 정의된 응답을 정의해 주시기 바랍니다.
            responseEntity = new ResponseEntity<>(e.toString(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return responseEntity;
    }

    @RequestMapping(value = "/api/v1/insertMany",
            consumes = "application/json",
            produces = "application/json",
            method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<String> insertMany(
            @RequestHeader HttpHeaders headers,
            @RequestBody Document bodyDoc) {

        ResponseEntity<String> responseEntity = null;

        try {
            if (logger.isDebugEnabled()) {
                logger.debug("========================================");
                logger.debug("HttpHeaders     : {}", headers.toString());
                logger.debug("RequestBody     : {}", bodyDoc.toString());
            }

            Document insertOneDoc = new Document(bodyDoc);

            // TODO : Controller - Service - DAO
            MongoDatabase testDB = mongoClient.getDatabase("test");
            MongoCollection<Document> sampleCollection = testDB.getCollection("sample");

            // InsertMany
            List<Document> grades = new ArrayList<>();
            for (double classId = 1d; classId <= 10d; classId++) {
                grades.add(generateNewGrade(10001d, classId));
            }

            InsertManyResult insertManyResult = sampleCollection.insertMany(grades, new InsertManyOptions().ordered(false));

            // Business 정의에 따라 error enum type 를 정의해서 front end 와 규약에 맞게 처리 바랍니다.
            if (insertManyResult.wasAcknowledged())
                responseEntity = new ResponseEntity<>("Insert Ok", HttpStatus.OK);
            else
                responseEntity = new ResponseEntity<>("Insert Fail", HttpStatus.NO_CONTENT);

        } catch (Exception e) {
            // Exception 에 따라 정의된 응답을 정의해 주시기 바랍니다.
            responseEntity = new ResponseEntity<>(e.toString(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return responseEntity;
    }


    @RequestMapping(value = "/api/v1/find",
            consumes = "application/json",
            produces = "application/json",
            method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<Document> find(
            @RequestHeader HttpHeaders headers,
            @RequestParam HashMap<String, String> paramsMap) {

        ResponseEntity<Document> responseEntity = null;

        try {

            if (logger.isDebugEnabled()) {
                logger.debug("========================================");
                logger.debug("HttpHeaders     : {}", headers.toString());
                logger.debug("RequestParam    : {}", paramsMap.toString());
            }

            Document findOneDoc = new Document();
            if (paramsMap != null && paramsMap.size() > 0)
                findOneDoc.append("_id", paramsMap.get("id"));

            if (logger.isDebugEnabled()) {
                logger.debug("========================================");
                logger.debug("findOneDoc    : {}", findOneDoc.toString());
                logger.debug("========================================");
            }

            // TODO : Controller - Service - DAO
            MongoDatabase testDB = mongoClient.getDatabase("test");
            MongoCollection<Document> sampleCollection = testDB.getCollection("sample");

            // find one document with new Document
            Document student1 = sampleCollection.find(new Document("student_id", 10000)).first();
            System.out.println("Student 1: " + student1.toJson());

            // find one document with Filters.eq()
            Document student2 = sampleCollection.find(eq("student_id", 10000)).first();
            System.out.println("Student 2: " + student2.toJson());

            // find a list of documents and iterate throw it using an iterator.
            FindIterable<Document> iterable = sampleCollection.find(gte("student_id", 10000));
            MongoCursor<Document> cursor = iterable.iterator();
            System.out.println("Student list with a cursor: ");
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }

            // find a list of documents and use a List object instead of an iterator
            List<Document> studentList = sampleCollection.find(gte("student_id", 10000)).into(new ArrayList<>());
            System.out.println("Student list with an ArrayList:");
            for (Document student : studentList) {
                System.out.println(student.toJson());
            }

            // find a list of documents and print using a consumer
            System.out.println("Student list using a Consumer:");
            Consumer<Document> printConsumer = document -> System.out.println(document.toJson());
            sampleCollection.find(gte("student_id", 10000)).forEach(printConsumer);

            // find a list of documents with sort, skip, limit and projection
            List<Document> docs = sampleCollection.find(and(eq("student_id", 10001), lte("class_id", 5)))
                    .projection(fields(excludeId(), include("class_id", "student_id")))
                    .sort(descending("class_id"))
                    .skip(2)
                    .limit(2)
                    .into(new ArrayList<>());

            System.out.println("Student sorted, skipped, limited and projected: ");
            for (Document student : docs) {
                System.out.println(student.toJson());
            }

            // Business 정의에 따라 error enum type 를 정의해서 front end 와 규약에 맞게 처리 바랍니다.
            if (!docs.isEmpty())
                responseEntity = new ResponseEntity<>(new Document("msg", "success :  Data all found"), HttpStatus.OK);
            else
                responseEntity = new ResponseEntity<>(new Document("msg", "Error : Not Found"), HttpStatus.NOT_FOUND);

        } catch (Exception e) {
            // Exception 에 따라 정의된 응답을 정의해 주시기 바랍니다.
            logger.error("{}", e.toString());
            responseEntity = new ResponseEntity<>(new Document("mgs", e.toString()), HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return responseEntity;
    }

    @RequestMapping(value = "/api/v1/update",
            consumes = "application/json",
            produces = "application/json",
            method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<String> update(
            @RequestHeader HttpHeaders headers,
            @RequestParam HashMap<String, String> paramsMap,
            @RequestBody Document bodyDoc) {

        ResponseEntity<String> responseEntity = null;

        try {

            if (logger.isDebugEnabled()) {
                logger.debug("========================================");
                logger.debug("HttpHeaders     : {}", headers.toString());
                logger.debug("RequestParam    : {}", paramsMap.toString());
                logger.debug("RequestBody     : {}", bodyDoc.toString());
            }

            Document updateFilter = new Document(bodyDoc);
            Document setDoc = new Document();
            Document valueDoc = new Document();
            valueDoc.putAll(paramsMap);

            if (paramsMap != null && paramsMap.size() > 0) {
                setDoc.append("$set", valueDoc);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("========================================");
                logger.debug("updateFilter  : {}", updateFilter.toString());
                logger.debug("setDoc       : {}", setDoc.toString());
                logger.debug("========================================");
            }

            // TODO : Controller - Service - DAO
            JsonWriterSettings prettyPrint = JsonWriterSettings.builder().indent(true).build();

            MongoDatabase testDB = mongoClient.getDatabase("test");
            MongoCollection<Document> sampleCollection = testDB.getCollection("sample");

            // update one document
            Bson filter = eq("student_id", 10000);
            Bson updateOperation = set("comment", "You should learn MongoDB!");
            UpdateResult updateResult = sampleCollection.updateOne(filter, updateOperation);
            System.out.println("=> Updating the doc with {\"student_id\":10000}. Adding comment.");
            System.out.println(sampleCollection.find(filter).first().toJson(prettyPrint));
            System.out.println(updateResult);

            // upsert
            filter = and(eq("student_id", 10002d), eq("class_id", 10d));
            updateOperation = push("comments", "You will learn a lot if you read the MongoDB blog!");
            UpdateOptions options = new UpdateOptions().upsert(true);
            updateResult = sampleCollection.updateOne(filter, updateOperation, options);
            System.out.println("\n=> Upsert document with {\"student_id\":10002.0, \"class_id\": 10.0} because it doesn't exist yet.");
            System.out.println(updateResult);
            System.out.println(sampleCollection.find(filter).first().toJson(prettyPrint));

            // update many documents
            filter = eq("student_id", 10001);
            updateResult = sampleCollection.updateMany(filter, updateOperation);
            System.out.println("\n=> Updating all the documents with {\"student_id\":10001}.");
            System.out.println(updateResult);

            // findOneAndUpdate
            filter = eq("student_id", 10000);
            Bson update1 = inc("x", 10); // increment x by 10. As x doesn't exist yet, x=10.
            Bson update2 = rename("class_id", "new_class_id"); // rename variable "class_id" in "new_class_id".
            Bson update3 = mul("scores.0.score", 2); // multiply the first score in the array by 2.
            Bson update4 = addToSet("comments", "This comment is uniq"); // creating an array with a comment.
            Bson update5 = addToSet("comments", "This comment is uniq"); // using addToSet so no effect.
            Bson updates = combine(update1, update2, update3, update4, update5);
            // returns the old version of the document before the update.
            Document oldVersion = sampleCollection.findOneAndUpdate(filter, updates);
            System.out.println("\n=> FindOneAndUpdate operation. Printing the old version by default:");
            System.out.println(oldVersion.toJson(prettyPrint));

            // but I can also request the new version
            filter = eq("student_id", 10001);
            FindOneAndUpdateOptions optionAfter = new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);
            Document newVersion = sampleCollection.findOneAndUpdate(filter, updates, optionAfter);
            System.out.println("\n=> FindOneAndUpdate operation. But we can also ask for the new version of the doc:");
            System.out.println(newVersion.toJson(prettyPrint));

            // Business 정의에 따라 error enum type 를 정의해서 front end 와 규약에 맞게 처리 바랍니다.
            if (updateResult.wasAcknowledged())
                responseEntity = new ResponseEntity<>("Update Ok", HttpStatus.OK);
            else
                responseEntity = new ResponseEntity<>("Update Fail", HttpStatus.NO_CONTENT);

        } catch (Exception e) {
            // Exception 에 따라 정의된 응답을 정의해 주시기 바랍니다.
            responseEntity = new ResponseEntity<>(e.toString(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return responseEntity;
    }


    @RequestMapping(value = "/api/v1/delete",
            consumes = "application/json",
            produces = "application/json",
            method = RequestMethod.DELETE)
    @ResponseBody
    public ResponseEntity<Document> deleteOne(
            @RequestHeader HttpHeaders headers,
            @RequestParam HashMap<String, String> paramsMap) {

        ResponseEntity<Document> responseEntity = null;

        try {

            if (logger.isDebugEnabled()) {
                logger.debug("========================================");
                logger.debug("HttpHeaders     : {}", headers.toString());
                logger.debug("RequestParam    : {}", paramsMap.toString());
            }

            Document findOneDoc = new Document();
            if (paramsMap != null && paramsMap.size() > 0)
                findOneDoc.putAll(paramsMap);

            if (logger.isDebugEnabled()) {
                logger.debug("========================================");
                logger.debug("findOneDoc    : {}", findOneDoc.toString());
                logger.debug("========================================");
            }

            // TODO : Controller - Service - DAO
            MongoDatabase testDB = mongoClient.getDatabase("test");
            MongoCollection<Document> sampleCollection = testDB.getCollection("sample");

            // delete one document
            Bson filter = eq("student_id", 10000);
            DeleteResult result = sampleCollection.deleteOne(filter);
            System.out.println(result);

            // findOneAndDelete operation
            filter = eq("student_id", 10002);
            Document doc = sampleCollection.findOneAndDelete(filter);
            System.out.println(doc.toJson(JsonWriterSettings.builder().indent(true).build()));

            // delete many documents
            filter = gte("student_id", 10000);
            result = sampleCollection.deleteMany(filter);
            System.out.println(result);

            // delete the entire collection and its metadata (indexes, chunk metadata, etc).
            //sampleCollection.drop();

            if (logger.isDebugEnabled()) {
                logger.debug("========================================");
                logger.debug("deleteResult    : {}", result.toString());
                logger.debug("========================================");
            }

            // Business 정의에 따라 error enum type 를 정의해서 front end 와 규약에 맞게 처리 바랍니다.
            if (!doc.isEmpty())
                responseEntity = new ResponseEntity<>(new Document("msg", "Delete One OK!"), HttpStatus.OK);
            else
                responseEntity = new ResponseEntity<>(new Document("msg", "Error : Not Found"), HttpStatus.NOT_FOUND);

        } catch (Exception e) {
            // Exception 에 따라 정의된 응답을 정의해 주시기 바랍니다.
            responseEntity = new ResponseEntity<>(new Document("mgs", e.toString()), HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return responseEntity;
    }


    @RequestMapping(value = "/api/v1/aggregatetemplate",
            consumes = "application/json",
            produces = "application/json",
            method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<Document> aggregateTemplate(
            @RequestHeader HttpHeaders headers,
            @RequestParam HashMap<String, String> paramsMap) {
        ResponseEntity<Document> responseEntity = null;

        try {
            if (logger.isDebugEnabled()) {
                logger.debug("========================================");
                logger.debug("HttpHeaders     : {}", headers.toString());
                logger.debug("RequestParam    : {}", paramsMap.toString());
            }

            Document findOneDoc = new Document();
            if (paramsMap != null && paramsMap.size() > 0)
                findOneDoc.append("_id", paramsMap.get("id"));

            if (logger.isDebugEnabled()) {
                logger.debug("========================================");
                logger.debug("findOneDoc    : {}", findOneDoc.toString());
                logger.debug("========================================");
            }

            // TODO : Controller - Service - DAO
            ArrayList<Document> reviews = new ArrayList<>();
            Criteria queryOne = new Criteria();
            MatchOperation matchOperation1 =
                    Aggregation.match(
                            queryOne.andOperator(
                                    Criteria.where("row_id").is("1234"),
                                    Criteria.where("cdate").gte("20201101"),
                                    Criteria.where("cdate").lt("20201102")
                            )
                    );

            // History Data 를 Unwind 로 풀어줌 1개의 Document 에 5개의 어레이가 있을 경우 5개의 Document 가 생성
            UnwindOperation unwindOperation =
                    Aggregation.unwind("history");

            // Flat 한 Document 로 만듬
            AggregationOperation replaceRoot =
                    Aggregation.replaceRoot().withValueOf(
                            ObjectOperators.valueOf("$history")
                                    .mergeWith(Aggregation.ROOT)
                    );

            Criteria queryTwo = new Criteria();
            // Array 내부의 시간 기반으로 필터링
            MatchOperation matchOperation2 =
                    Aggregation.match(
                            queryTwo.andOperator(
                                    Criteria.where("create_date").gte("20201102000000"),
                                    Criteria.where("create_date").lte("20201102235959")
                            )
                    );

            // 필요없는 _id 필드와 어레이 필드 history 제거
            ProjectionOperation projectionOperation =
                    Aggregation.project().andExclude("_id").andExclude("history");

            // Aggregation pipeline 생성
            Aggregation aggregation = Aggregation.newAggregation(
                    matchOperation1,
                    unwindOperation,
                    replaceRoot,
                    matchOperation2,
                    projectionOperation
            );

            logger.error("{}", aggregation.toString());

            // Aggregation 실행 Document 는 매핑 가능한 Value Object 또는 DTO Class 가 있다면 대체 가능
            AggregationResults<Document> aggregationResults =
                    mongoTemplate.aggregate(
                            aggregation,
                            "history",
                            Document.class
                    );

            // Return Result 형태에 추가
            for (Document document : aggregationResults) {
                logger.error("{}", document.toString());
                reviews.add(document);
            }

            // Business 정의에 따라 error enum type 를 정의해서 front end 와 규약에 맞게 처리 바랍니다.
            if (!reviews.isEmpty())
                responseEntity = new ResponseEntity<>(new Document("msg", "success :  Data all found"), HttpStatus.OK);
            else
                responseEntity = new ResponseEntity<>(new Document("msg", "Error : Not Found"), HttpStatus.NOT_FOUND);
        } catch (Exception e) {
            // Exception 에 따라 정의된 응답을 정의해 주시기 바랍니다.
            logger.error("{}", e.toString());
            responseEntity = new ResponseEntity<>(new Document("mgs", e.toString()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return responseEntity;
    }

    @RequestMapping(value = "/api/v1/aggregate",
            consumes = "application/json",
            produces = "application/json",
            method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<Document> aggregate(
            @RequestHeader HttpHeaders headers,
            @RequestParam HashMap<String, String> paramsMap) {
        ResponseEntity<Document> responseEntity = null;

        try {
            if (logger.isDebugEnabled()) {
                logger.debug("========================================");
                logger.debug("HttpHeaders     : {}", headers.toString());
                logger.debug("RequestParam    : {}", paramsMap.toString());
            }

            Document findOneDoc = new Document();
            if (paramsMap != null && paramsMap.size() > 0)
                findOneDoc.append("_id", paramsMap.get("id"));

            if (logger.isDebugEnabled()) {
                logger.debug("========================================");
                logger.debug("findOneDoc    : {}", findOneDoc.toString());
                logger.debug("========================================");
            }

            // TODO : Controller - Service - DAO
            ArrayList<Document> reviews = new ArrayList<>();

            MongoDatabase database = mongoClient.getDatabase("test");
            MongoCollection<Document> collection = database.getCollection("history");

            ArrayList<Document> aggregateStages = new ArrayList<>();
            Document match = new Document("$match",
                    new Document("$and", Arrays.asList(
                            new Document("row_id", "1234"),
                            new Document("cdate", new Document("$gte", "20201101")),
                            new Document("cdate", new Document("$lt", "20201102")))
                    ));

            Document unwind = new Document("$unwind", "$history");

            Document replaceRoot = new Document("$replaceRoot",
                    new Document("newRoot", new Document("$mergeObjects", Arrays.asList("$history", "$$ROOT"))));

            Document match2 = new Document("$match",
                    new Document("$and", Arrays.asList(
                            new Document("create_date", new Document("$gte", "20201102000000")),
                            new Document("create_date", new Document("$lte", "20201102235959")))
                    ));

            Document project = new Document("$project",
                    new Document("_id", 0L).append("history", 0L));

            aggregateStages.add(match);
            aggregateStages.add(unwind);
            aggregateStages.add(replaceRoot);
            aggregateStages.add(match2);
            aggregateStages.add(project);

            logger.error("{}", aggregateStages.toString());
            MongoCursor<Document> cursor = collection.aggregate(aggregateStages).iterator();

            while (cursor.hasNext()) {
                reviews.add(cursor.next());
            }

            // Business 정의에 따라 error enum type 를 정의해서 front end 와 규약에 맞게 처리 바랍니다.
            if (!reviews.isEmpty())
                responseEntity = new ResponseEntity<>(new Document("msg", "success :  Data all found"), HttpStatus.OK);
            else
                responseEntity = new ResponseEntity<>(new Document("msg", "Error : Not Found"), HttpStatus.NOT_FOUND);
        } catch (Exception e) {
            // Exception 에 따라 정의된 응답을 정의해 주시기 바랍니다.
            logger.error("{}", e.toString());
            responseEntity = new ResponseEntity<>(new Document("mgs", e.toString()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return responseEntity;
    }
}