import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

import java.util.List;
import java.lang.String;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;

import org.apache.spark.sql.Encoders;

import java.util.Iterator;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;

import java.lang.Integer;

import org.apache.spark.sql.Encoder;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.GeneralizedLinearRegression;
import org.apache.spark.ml.regression.GeneralizedLinearRegressionModel;
import org.apache.spark.ml.regression.GeneralizedLinearRegressionTrainingSummary;

import java.util.Arrays;

import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;

public class Spark {
    public static Dataset<Row> mostFrequent(Dataset<Row> q2result) {
        return q2result.groupBy("user_email").agg(functions.max("post_count"));
    }

    public static Dataset<Row> highestAve(Dataset<Row> q2result) {
        return q2result.groupBy("user_email").agg(functions.max("average"));
    }

    public static Dataset<Row> getUserDataset(Dataset<Row> users, Dataset<Row> frequent_categ,
                                              Dataset<Row> highest_ave, Dataset<Row> q2result) {

        Dataset<Row> firstJoin = users.withColumnRenamed("email", "user_email")
                .join(frequent_categ, "user_email");

        Dataset<Row> secondJoin = firstJoin.join(highest_ave, "user_email");

        Dataset<Row> droppedCol = secondJoin.drop("fullname");

        Dataset<Row> droppedQ2Col = q2result.drop("likes_count");

        Dataset<Row> thirdJoin = droppedCol.join(droppedQ2Col, "user_email");

        Dataset<User> user_dataset = thirdJoin.flatMap(new FlatMapFunction<Row, User>() {
            @Override
            public Iterator<User> call(Row row) throws Exception {
                List<User> user_list_flat = new ArrayList<User>();
                User user_obj = new User();

                String user_email = row.get(0).toString();
                user_obj.setEmail(user_email);

                int age = Integer.parseInt(row.get(1).toString());
                user_obj.setAge(age);

                int followers_count = Integer.parseInt(row.get(2).toString());
                user_obj.setFollowersCount(followers_count);

                String cat_max_post_count = null;
                int max_post_count = Integer.parseInt(row.get(3).toString());
                int post_count = Integer.parseInt(row.get(6).toString());
                String category = row.get(5).toString();
                if (max_post_count == post_count) {
                    cat_max_post_count = category;
                }

                user_obj.setCatMaxPostCount(cat_max_post_count);
                float max_average = (float) row.get(4);
                float average = (float) row.get(7);
                String cat_max_avg_post_likes = null;
                if (max_average == average) {
                    cat_max_avg_post_likes = category;
                }
                user_obj.setCatMaxAvgPostLikes(cat_max_avg_post_likes);
                user_list_flat.add(user_obj);

                return user_list_flat.iterator();
            }
        }, Encoders.bean(User.class));

        Dataset<Row> groupped = user_dataset.groupBy("email", "age", "followersCount")
                .agg(functions.first("catMaxPostCount", true)
                        .alias("cat_max_post_count"), functions.first("catMaxAvgPostLikes", true)
                        .alias("cat_max_avg_post_likes"));
        return groupped;
    }

    public static void linearRegresionMethod(Dataset<Row> user_dataset) {

        StringIndexer indexer = new StringIndexer().setInputCol("cat_max_post_count")
                .setOutputCol("catMaxPostCountIndex");
        Dataset<Row> indexed = indexer.fit(user_dataset).transform(user_dataset);

        StringIndexer indexerSecond = new StringIndexer().setInputCol("cat_max_avg_post_likes").setOutputCol("catMaxAvgPostLikesIndex");
        Dataset<Row> indexedSecond = indexerSecond.fit(indexed).transform(indexed);

        Dataset<Row>[] splits = indexedSecond.randomSplit(new double[]{0.7, 0.3});

        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        VectorAssembler assembler =new VectorAssembler().setInputCols(
                new String[]{"followersCount", "catMaxPostCountIndex",
                "catMaxAvgPostLikesIndex"}).setOutputCol("features");

        Dataset<Row>output=assembler.transform(trainingData);

        LinearRegression lr = new LinearRegression().setLabelCol("age").
                setFeaturesCol("features");

        ParamMap[] paramGrid = new ParamGridBuilder().
                addGrid(lr.regParam(), new double[]{0.05, 0.3, 0.7})
                .addGrid(lr.elasticNetParam(), new double[]{0.2, 0.6, 0.9}).build();

        RegressionEvaluator evaluator = new RegressionEvaluator().setLabelCol("age")
                .setPredictionCol("prediction").setMetricName("rmse");

        CrossValidator cv = new CrossValidator().setEstimator(lr).setEvaluator(evaluator)
                .setEstimatorParamMaps(paramGrid).setNumFolds(5);

        CrossValidatorModel cvModel = cv.fit(output);

        Dataset<Row> outputTest = assembler.transform(testData);
        Dataset<Row> predictions = cvModel.transform(outputTest);
        double rmse = evaluator.evaluate(predictions);

        // Fit the model.
        LinearRegression secondlr = new LinearRegression().setLabelCol("age").setFeaturesCol("features").setRegParam(0.6).setElasticNetParam(0.9);
        LinearRegressionModel lrModel = secondlr.fit(output);
        LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
        System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);
        // Print the coefficients and intercept for linear regression.
        System.out.println("Coefficients: " + lrModel.coefficients() + " Intercept: " + lrModel.intercept());
        System.out.println("numIterations: " + trainingSummary.totalIterations());
        System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
        trainingSummary.residuals().show();
    }

    public static void generalizedLRegresionMethod(Dataset<Row> user_dataset) {
        StringIndexer indexer = new StringIndexer().setInputCol("cat_max_post_count").setOutputCol("catMaxPostCountIndex");
        Dataset<Row> indexed = indexer.fit(user_dataset).transform(user_dataset);
        StringIndexer indexerSecond = new StringIndexer().setInputCol("cat_max_avg_post_likes").setOutputCol("catMaxAvgPostLikesIndex");
        Dataset<Row> indexedSecond = indexerSecond.fit(indexed).transform(indexed);
        Dataset<Row>[] splits = indexedSecond.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];
        VectorAssembler assembler = new VectorAssembler().setInputCols(new String[]{"followersCount", "catMaxPostCountIndex", "catMaxAvgPostLikesIndex"}).setOutputCol("features");
        Dataset<Row> output = assembler.transform(trainingData);
        GeneralizedLinearRegression glr = new GeneralizedLinearRegression().setLabelCol("age").setFeaturesCol("features").setFamily("gaussian").setLink("identity");
        ParamMap[] paramGrid = new ParamGridBuilder().addGrid(glr.regParam(), new double[]{0.05, 0.3, 0.7}).build();
        RegressionEvaluator evaluator = new RegressionEvaluator().setLabelCol("age").setPredictionCol("prediction").setMetricName("rmse");
        CrossValidator cv = new CrossValidator().setEstimator(glr).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(5);
        CrossValidatorModel cvModel = cv.fit(output);
        Dataset<Row> outputTest = assembler.transform(testData);
        Dataset<Row> predictions = cvModel.transform(outputTest);
        double rmse = evaluator.evaluate(predictions);
        // Fit the model
        GeneralizedLinearRegression secondglr = new GeneralizedLinearRegression().setLabelCol("age").setFeaturesCol("features").setFamily("gaussian").setLink("identity").setRegParam(0.6);
        GeneralizedLinearRegressionModel model = secondglr.fit(output);
        // Print the coefficients and intercept for generalized linear regression model
        System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);
        System.out.println("Coefficients: " + model.coefficients());
        System.out.println("Intercept: " + model.intercept());
        // Summarize the model over the training set and print out some metrics
        GeneralizedLinearRegressionTrainingSummary summary = model.summary();
        System.out.println("Coefficient Standard Errors: " + Arrays.toString(summary.coefficientStandardErrors()));
        System.out.println("T Values: " + Arrays.toString(summary.tValues()));
        System.out.println("P Values: " + Arrays.toString(summary.pValues()));
        System.out.println("Dispersion: " + summary.dispersion());
        summary.residuals().show();
    }

    public static void dTreeRegresionMethod(Dataset<Row> user_dataset) {
        StringIndexer indexer = new StringIndexer().
                setInputCol("cat_max_post_count").setOutputCol("catMaxPostCountIndex");
        Dataset<Row> indexed = indexer.fit(user_dataset).transform(user_dataset);
        StringIndexer indexerSecond = new StringIndexer().
                setInputCol("cat_max_avg_post_likes").setOutputCol("catMaxAvgPostLikesIndex");

        Dataset<Row> indexedSecond = indexerSecond.fit(indexed).transform(indexed);

        indexedSecond.show();

        Dataset<Row>[] splits = indexedSecond.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        VectorAssembler assembler = new VectorAssembler().
                setInputCols(new String[]{"followersCount", "catMaxPostCountIndex", "catMaxAvgPostLikesIndex"}).
                setOutputCol("features");

        Dataset<Row> output = assembler.transform(trainingData);

        DecisionTreeRegressor dt = new DecisionTreeRegressor().setLabelCol("age").setFeaturesCol("features");
        RegressionEvaluator evaluator = new RegressionEvaluator().setLabelCol("age").
                setPredictionCol("prediction").setMetricName("rmse");

        ParamMap[] paramGrid = new ParamGridBuilder().
                addGrid(dt.maxDepth(), new int[]{5, 7, 12}).build();

        CrossValidator cv = new CrossValidator().setEstimator(dt).setEvaluator(evaluator).
                setEstimatorParamMaps(paramGrid).setNumFolds(5);

        CrossValidatorModel cvModel = cv.fit(output);

        Dataset<Row> outputTest = assembler.transform(testData);
        Dataset<Row> predictions = cvModel.transform(outputTest);

        predictions.show();

        double rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Spark").master("local").getOrCreate();
        Dataset<Row> users = spark.read().schema("email string, fullname string, age int, followers_count int").csv("/social-networkInput/users.csv");
        Dataset<Row> q2result = spark.read().schema("user_email string,category string, post_count int, likes_count int, average float").csv("/social-networkInput/q2result.csv");
        Dataset<Row> frequent_categ = mostFrequent(q2result);
        Dataset<Row> highest_ave = highestAve(q2result);
        // New Dataset: USER(email, age, followers count, cat_max_post count,cat_max_avg post likes)
        Dataset<Row> user_dataset = getUserDataset(users, frequent_categ, highest_ave, q2result);

        user_dataset.show();
        //linearRegresionMethod(user_dataset);
        //generalizedLRegresionMethod(user_dataset);
        dTreeRegresionMethod(user_dataset);
        spark.stop();
    }
}