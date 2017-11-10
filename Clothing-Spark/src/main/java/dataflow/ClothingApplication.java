package dataflow;

import utility.Property;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.api.java.JavaRDD;
import utility.Utility;

/**
 *
 * @author vitor
 */
public class ClothingApplication {

    public static void main(String[] args) {
        runApplication();
    }
    
    public static void runApplication(){
        // File with properties
        Property property = new Property();
        
        // Removing directory with output files from previous executions
        String outputDir = property.getWorkspace() + "/" + Utility.OUTPUT_DIRECTORY;
        String outputPath = "file://" + outputDir + "/";
        Utility.runCommandLine("rm -rf " + outputDir + "/*");
        Utility.makeDir(outputDir);
        
        Logger logger = Logger.getLogger("Clothing Company Application");
        
        // Now set its level. Normally you do not need to set the
        // level of a logger programmatically. This is usually done
        // in configuration files.
        logger.setLevel(Level.WARNING);
        
        // Paths
        String inputPath = property.getWorkspace() + "/input_dataset/";

        System.out.println("# Initializing dataflow");
        Dataflow dataflow = new Dataflow(property, inputPath, outputPath);
        
        System.out.println("# Transformation 0 - Loading customer lists into RDD");
        dataflow.loadCustomerLists();
        
        System.out.println("# Transformation 1 - Deduplication");
        JavaRDD<String> deduplication = dataflow.deduplication(
                "deduplication");
        
        System.out.println("# Transformation 2 - Filter by countries");
        System.out.println(" --> united states");
        JavaRDD<String> us = dataflow.filter(deduplication, 1, "united_states");
        
        System.out.println(" --> europe");
        JavaRDD<String> eu = dataflow.filter(deduplication, 2, "europe");
        
        //unpersist
        deduplication.unpersist();
       
        System.out.println("# Transformation 3 - Union of tuples from United States and Europe");
        JavaRDD<String> union = dataflow.union("union", us, eu);
        
        System.out.println("# Transformation 4 - Cartesian product of customers with cloth items");
        System.out.println(" --> loading cloth items into RDD");
        dataflow.loadClothItems();
        
        System.out.println(" --> cartesian product");
        JavaRDD<String> cartesianProduct = dataflow.cartesianProduct("cartesian_product", union);
        
        System.out.println("# Transformation 5 - Prediction");
        System.out.println(" --> loading buying patterns into RDD");
        dataflow.loadBuyingPatterns();
        
        System.out.println(" --> prediction");
        JavaRDD<String> prediction = dataflow.prediction("prediction", cartesianProduct);
        
        System.out.println("# Transformation 6 - Aggregation");
        JavaRDD<String> aggregation = dataflow.aggregation("aggregation", prediction);
        
        System.out.println("# End");
    }

}
