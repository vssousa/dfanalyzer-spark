package dataflow;

import utility.Property;
import function.FilterFunction;
import function.SimilarityFunction;
import java.text.DecimalFormat;
import utility.Utility;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pde.PDE;
import scala.Tuple2;

/**
 *
 * @author vitor
 */
public class Dataflow {

    private final JavaSparkContext sparkContext;
    private final String sparkInputWorkspace;
    private final String sparkOutputWorkspace;
    private final String outputWorkspace;

    private JavaRDD<String> customerList1;
    private JavaRDD<String> customerList2;
    private JavaRDD<String> customerList3;
    private JavaRDD<String> customerList4;

    private JavaRDD<String> buyingPatterns; // buying patterns file
    private JavaRDD<String> clothesItems; // clothes items

    Property config; // dfanalyzer property
    String dataflowTag;
    String fastbitBinPath;
    PDE pde;

    public Dataflow(Property dfaProperty, String inputPath, String outputPath) {
        this.config = dfaProperty;
        this.dataflowTag = this.config.getDataflowTag();
        this.fastbitBinPath = this.config.getFastBitBinPath();
        this.pde = new PDE(this.config.getURL());

        SparkConf conf = new SparkConf()
                .setAppName("Clothing Company Application");
        if (this.config.debug()) {
            conf.setMaster("local[2]");
        }

        sparkContext = new JavaSparkContext(conf);
        if (this.config.debug()) {
            sparkContext.setLogLevel("ERROR");
        }

        this.sparkOutputWorkspace = outputPath;
        this.sparkInputWorkspace = inputPath;
        this.outputWorkspace = outputPath.replaceAll("file://", "");
    }

    /**
     * It loads customer lists into RDD
     */
    void loadCustomerLists() {
        //spark - read input datasets
        String[] inputDatasets = new String[]{
            "customer_list1", "customer_list2", "customer_list3", "customer_list4"};
        this.customerList1 = sparkContext.textFile(sparkInputWorkspace + inputDatasets[0] + ".txt");
        this.customerList2 = sparkContext.textFile(sparkInputWorkspace + inputDatasets[1] + ".txt");
        this.customerList3 = sparkContext.textFile(sparkInputWorkspace + inputDatasets[2] + ".txt");
        this.customerList4 = sparkContext.textFile(sparkInputWorkspace + inputDatasets[3] + ".txt");

        for (String inputDataset : inputDatasets) {
            //rde and rdi
            scientificDataCapture(inputDataset);
        }
    }

    /**
     * It loads cloth items into RDD
     */
    void loadClothItems() {
        //spark - read input dataset
        String inputDataset = "cloth_items";
        this.clothesItems = sparkContext.textFile(sparkInputWorkspace + inputDataset + ".txt");
        //rde and rdi
        scientificDataCapture(inputDataset);
    }

    /**
     * It loads buying patterns into RDD
     */
    void loadBuyingPatterns() {
        //spark - read input dataset
        String inputDataset = "buying_patterns";
        this.buyingPatterns = sparkContext.textFile(sparkInputWorkspace + inputDataset + ".txt");
        //rde and rdi
        scientificDataCapture(inputDataset);
    }

    /**
     * It does the union of the RDD and remove duplicated tuples (customers).
     * This method does not reduce.
     */
    public JavaRDD<String> deduplication(String transformationTag) {
        String inputDataset = Utility.getInputDataset(transformationTag);
        String outputDataset = Utility.getOutputDataset(transformationTag);

        //dfanalyzer
        //pde
        pde.task(dataflowTag, transformationTag, config.getTaskID(), "RUNNING",
                outputWorkspace, config.getResource());
        pde.collection(inputDataset,
                "{{" + getFileElement("customer_list1") + ";"
                + getFileElement("customer_list2") + ";"
                + getFileElement("customer_list3") + ";"
                + getFileElement("customer_list4") + "}}");
        pde.sendRequest();
        
        //spark
        //processing
        JavaRDD<String> deduplication;
        deduplication = customerList1.union(customerList2);
        deduplication.cache();
        deduplication = deduplication.union(customerList3);
        deduplication.cache();
        deduplication = deduplication.union(customerList4).distinct();
        deduplication.cache();
        //write rdd
        deduplication.saveAsTextFile(outputWorkspace + transformationTag);
        //unpersist
        customerList1.unpersist();
        customerList2.unpersist();
        customerList3.unpersist();
        customerList4.unpersist();

        //dfanalyzer
        //rde and rdi
        scientificDataCapture(transformationTag);
        //pde
        pde.changeTaskStatus("FINISHED");
        pde.collection(outputDataset, "{{" + getFileElement(transformationTag) + "}}");
        pde.sendRequest();

        config.clearTask();
        pde.clearMessage();
        return deduplication;
    }

    /**
     * It filters customers by their country
     */
    public JavaRDD<String> filter(JavaRDD<String> rdd, int attributeIndex, String transformationTag) {
        String outputDataset = Utility.getOutputDataset(transformationTag);

        //dfanalyzer
        //pde
        pde.task(dataflowTag, transformationTag, config.getTaskID(), "RUNNING",
                outputWorkspace, config.getResource());
        pde.dependency("{deduplication}", "{" + config.getTaskID() + "}");
        pde.sendRequest();

        //spark
        //processing
        JavaRDD<String> filterByCountry = rdd.filter(
                new FilterFunction(attributeIndex, transformationTag)
        );
        //write rdd
        filterByCountry.saveAsTextFile(outputWorkspace + transformationTag);

        //dfanalyzer
        //rde and rdi
        scientificDataCapture(transformationTag);
        //pde
        pde.changeTaskStatus("FINISHED");
        pde.collection(outputDataset, "{{" + getFileElement(transformationTag) + "}}");
        pde.sendRequest();

        config.clearTask();
        pde.clearMessage();
        return filterByCountry;
    }

    public JavaRDD<String> union(String transformationTag,
            JavaRDD<String> innerRDD, JavaRDD<String> outerRDD) {
        String outputDataset = Utility.getOutputDataset(transformationTag);

        //dfanalyzer
        //pde
        pde.task(dataflowTag, transformationTag, config.getTaskID(), "RUNNING",
                outputWorkspace, config.getResource());
        pde.dependency("{europe;united_states}", 
                "{{" + config.getTaskID() + ";" + config.getTaskID() + "}}");
        pde.sendRequest();

        //spark
        //processing
        JavaRDD<String> union = innerRDD.union(outerRDD);
        //write rdd
        union.saveAsTextFile(outputWorkspace + transformationTag);
        //unpersist
        innerRDD.unpersist();
        outerRDD.unpersist();

        //dfanalyzer
        //rde and rdi
        scientificDataCapture(transformationTag);
        //pde
        pde.changeTaskStatus("FINISHED");
        pde.collection(outputDataset, "{{" + getFileElement(transformationTag) + "}}");
        pde.sendRequest();

        pde.clearMessage();
        config.clearTask();
        return union;
    }

    /**
     *
     * @param customerRDD, customers RDD
     * @return JavaPairRDD<String, String>, the cartesian product between the
     * two parameters
     */
    public JavaRDD<String> cartesianProduct(String transformationTag, JavaRDD<String> customerRDD) {
        String inputDataset = Utility.getInputDataset(transformationTag);
        String outputDataset = Utility.getOutputDataset(transformationTag);

        //dfanalyzer
        //pde
        pde.task(dataflowTag, transformationTag, config.getTaskID(), "RUNNING",
                outputWorkspace, config.getResource());
        pde.collection(inputDataset, "{{" + getFileElement("cloth_items") + "}}");
        pde.dependency("{union}", "{" + config.getTaskID() + "}");
        pde.sendRequest();

        //spark
        //processing
        JavaPairRDD<String, String> cartesian = customerRDD.cartesian(clothesItems);
        // JavaPairRDD to JavaRDD
        JavaRDD<String> rdd = cartesian.map(
                v
                -> v._1().split(Utility.SEPARATOR)[0] + Utility.SEPARATOR
                + v._1().split(Utility.SEPARATOR)[1] + Utility.SEPARATOR
                + v._1().split(Utility.SEPARATOR)[2] + Utility.SEPARATOR
                + v._1().split(Utility.SEPARATOR)[3] + Utility.SEPARATOR
                + v._1().split(Utility.SEPARATOR)[4] + Utility.SEPARATOR
                + v._1().split(Utility.SEPARATOR)[5] + Utility.SEPARATOR
                + v._1().split(Utility.SEPARATOR)[6] + Utility.SEPARATOR
                + v._2().split(Utility.SEPARATOR)[0] + Utility.SEPARATOR
                + v._2().split(Utility.SEPARATOR)[1]
        );
        //write rdd
        rdd.saveAsTextFile(outputWorkspace + transformationTag);
        //unpersist
        customerRDD.unpersist();
        clothesItems.unpersist();

        //dfanalyzer
        //rde and rdi
        scientificDataCapture(transformationTag);
        //pde
        pde.changeTaskStatus("FINISHED");
        pde.collection(outputDataset, "{{" + getFileElement(transformationTag) + "}}");
        pde.sendRequest();

        pde.clearMessage();
        config.clearTask();
        return cartesian.map(x -> x._1 + ";" + x._2);
    }

    /**
     * Consult the buying pattern to find out what clothes will probably buy a
     * customer given is the input Use buying-patterns.txt TODO write in a file
     *
     * @param customersRDD
     * @return
     */
    public JavaRDD<String> prediction(String transformationTag, JavaRDD<String> customersRDD) {
        String inputDataset = Utility.getInputDataset(transformationTag);
        String outputDataset = Utility.getOutputDataset(transformationTag);

        //dfanalyzer
        //pde
        pde.task(dataflowTag, transformationTag, config.getTaskID(), "RUNNING",
                outputWorkspace, config.getResource());
        pde.collection(inputDataset, "{{" + getFileElement("buying_patterns") + "}}");
        pde.dependency("{cartesian_product}", "{" + config.getTaskID() + "}");
        pde.sendRequest();

        //spark
        //processing
        JavaRDD<Tuple2<String, String>> cartesianProduct = customersRDD.cartesian(buyingPatterns).rdd().toJavaRDD();
        // calculation of similarities
        JavaRDD<String> similarities = cartesianProduct.map(new SimilarityFunction());
        JavaPairRDD<String, Tuple2<String, Double>> similaritiesPair = similarities.mapToPair(
                e -> new Tuple2<String, Tuple2<String, Double>>(
                        e.split(Utility.SEPARATOR)[0] + Utility.SEPARATOR
                        + e.split(Utility.SEPARATOR)[2],
                        new Tuple2<>(e.split(Utility.SEPARATOR)[1], Double.parseDouble(e.split(Utility.SEPARATOR)[3])))
        );
        // getting the maximum similarity between buying pattern and customer consumption
        similaritiesPair = similaritiesPair.reduceByKey(
                (v1, v2) -> new Tuple2<>(
                        (v1._2() > v2._2()) ? v1._1() : v2._1(),
                        Math.max(v1._2(), v2._2())
                )
        );
        JavaRDD<String> prediction = similaritiesPair.map(
                v
                -> v._1().split(Utility.SEPARATOR)[0] + Utility.SEPARATOR
                + v._2()._1() + Utility.SEPARATOR
                + v._1().split(Utility.SEPARATOR)[1] + Utility.SEPARATOR
                + (new DecimalFormat("#0.00")).format(v._2()._2()));
        //write rdd
        prediction.saveAsTextFile(outputWorkspace + transformationTag);
        //unpersist
        customersRDD.unpersist();
        buyingPatterns.unpersist();

        //dfanalyzer
        //rde and rdi
        scientificDataCapture(transformationTag);
        //pde
        pde.changeTaskStatus("FINISHED");
        pde.collection(outputDataset, "{{" + getFileElement(transformationTag) + "}}");
        pde.sendRequest();

        pde.clearMessage();
        config.clearTask();
        return prediction;
    }

    JavaRDD<String> aggregation(String transformationTag, JavaRDD<String> rdd) {
        String outputDataset = Utility.getOutputDataset(transformationTag);

        //dfanalyzer
        //pde
        pde.task(dataflowTag, transformationTag, config.getTaskID(), "RUNNING",
                outputWorkspace, config.getResource());
        pde.dependency("{prediction}", "{" + config.getTaskID() + "}");
        pde.sendRequest();

        //spark
        //processing
        // to separate cloth id and sale probability
        JavaPairRDD<String, Double> pair
                = rdd.mapToPair(
                        e -> new Tuple2<>(e.split(Utility.SEPARATOR)[2],
                                Double.parseDouble(e.split(Utility.SEPARATOR)[3])
                        )
                );
        // get quantities of cloth items to be sold
        pair = pair.reduceByKey((sum, newValue) -> (sum + (newValue * 10)));
        JavaRDD<String> aggregation = pair.map(v -> v._1() + Utility.SEPARATOR + String.valueOf(v._2().intValue()));
        //write rdd
        aggregation.saveAsTextFile(outputWorkspace + transformationTag);

        //dfanalyzer
        //rde and rdi
        scientificDataCapture(transformationTag);
        //pde
        pde.changeTaskStatus("FINISHED");
        pde.collection(outputDataset, "{{" + getFileElement(transformationTag) + "}}");
        pde.sendRequest();

        pde.clearMessage();
        config.clearTask();
        return aggregation;
    }

    public JavaSparkContext getSparkContext() {
        return sparkContext;
    }

    private String getFileElement(String transformationTag) {
        return outputWorkspace + transformationTag + "/" + transformationTag + ".data";
    }

    public void scientificDataCapture(String transformationTag) {
        //rde
        String program = "python ../bin/rde/extractor.py " + transformationTag;
        Utility.runCommandLine(outputWorkspace + "../dfa/RDE-1.0 PROGRAM:EXTRACT "
                + transformationTag + " " + outputWorkspace + " "
                + "\"" + program + "\" "
                + Utility.getAttMappings(transformationTag));
        //rdi
//        Utility.runCommandLine(outputWorkspace + "../dfa/RDI-1.0 OPTIMIZED_FASTBIT:INDEX "
//                + transformationTag + " " + outputWorkspace + transformationTag + " "
//                + transformationTag + ".data "
//                + Utility.getAttMappings(transformationTag)
//                + " -delimiter=\",\""
//                + " -bin=\"" + fastbitBinPath + "\"");
    }

}
