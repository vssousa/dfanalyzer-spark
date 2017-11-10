package utility;

import attribute.Attribute;
import attribute.BuyingPatternAttribute;
import attribute.CustomerAttribute;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 *
 * @author jean, vitor
 */
public class Utility {

    public static final String SEPARATOR = ";";
    public static final String PROPERTY_FILE_NAME = "property.config";
    public static final String FILE_PATH_SEPARATOR = "/";
    public static final String OUTPUT_DIRECTORY = "output";
    private static final int diskSyncTimeInMilliseconds = 3000;

    private static final Map<String, String> datasetAttributes;
    private static final Map<String, String> datasetTextAttributes;
    private static final Map<String, String> datasetIndexingAttributes;
    public static final ArrayList<String> rdiKeys = new ArrayList<>();

    static {
        datasetIndexingAttributes = new HashMap<String, String>();
        datasetIndexingAttributes.put("customer_list1", "[FILENAME:text,CUSTOMERID:numeric,COUNTRY:text,CONTINENT:text,AGE:numeric,GENDER:text,CHILDREN:numeric,STATUS:text]");
        datasetIndexingAttributes.put("customer_list2", "[FILENAME:text,CUSTOMERID:numeric,COUNTRY:text,CONTINENT:text,AGE:numeric,GENDER:text,CHILDREN:numeric,STATUS:text]");
        datasetIndexingAttributes.put("customer_list3", "[FILENAME:text,CUSTOMERID:numeric,COUNTRY:text,CONTINENT:text,AGE:numeric,GENDER:text,CHILDREN:numeric,STATUS:text]");
        datasetIndexingAttributes.put("customer_list4", "[FILENAME:text,CUSTOMERID:numeric,COUNTRY:text,CONTINENT:text,AGE:numeric,GENDER:text,CHILDREN:numeric,STATUS:text]");
        datasetIndexingAttributes.put("deduplication", "[FILENAME:text,CUSTOMERID:numeric,COUNTRY:text,CONTINENT:text,AGE:numeric,GENDER:text,CHILDREN:numeric,STATUS:text]");
        datasetIndexingAttributes.put("united_states", "[FILENAME:text,CUSTOMERID:numeric,COUNTRY:text,CONTINENT:text,AGE:numeric,GENDER:text,CHILDREN:numeric,STATUS:text]");
        datasetIndexingAttributes.put("europe", "[FILENAME:text,CUSTOMERID:numeric,COUNTRY:text,CONTINENT:text,AGE:numeric,GENDER:text,CHILDREN:numeric,STATUS:text]");
        datasetIndexingAttributes.put("union", "[FILENAME:text,CUSTOMERID:numeric,COUNTRY:text,CONTINENT:text,AGE:numeric,GENDER:text,CHILDREN:numeric,STATUS:text]");
        datasetIndexingAttributes.put("cloth_items", "[FILENAME:text,CLOTHID:numeric,DESCRIPTION:text]");
        datasetIndexingAttributes.put("cartesian_product", "[FILENAME:text,CUSTOMERID:numeric,COUNTRY:text,CONTINENT:text,AGE:numeric,GENDER:text,CHILDREN:numeric,STATUS:text,CLOTHID:numeric,DESCRIPTION:text]");
        datasetIndexingAttributes.put("buying_patterns", "[FILENAME:text,BUYINGPATTERNID:numeric,CLOTHID:numeric,COUNTRY:text,CONTINENT:text,AGE:numeric,GENDER:text,CHILDREN:numeric,STATUS:text]");
        datasetIndexingAttributes.put("prediction", "[FILENAME:text,CUSTOMERID:numeric,BUYINGPATTERNID:numeric,CLOTHID:numeric,PROBABILITY:numeric:2]");
        datasetIndexingAttributes.put("aggregation", "[FILENAME:text,CLOTHID:numeric,QUANTITY:numeric]");
    }

    public static String getAttMappings(String transformationTag) {
        return datasetIndexingAttributes.get(transformationTag);
    }

    static {
        datasetTextAttributes = new HashMap<String, String>();
        datasetTextAttributes.put("customer_list1", "1;2;4;6");
        datasetTextAttributes.put("customer_list2", "1;2;4;6");
        datasetTextAttributes.put("customer_list3", "1;2;4;6");
        datasetTextAttributes.put("customer_list4", "1;2;4;6");
        datasetTextAttributes.put("deduplication", "1;2;4;6");
        datasetTextAttributes.put("united_states", "1;2;4;6");
        datasetTextAttributes.put("europe", "1;2;4;6");
        datasetTextAttributes.put("union", "1;2;4;6");
        datasetTextAttributes.put("cloth_items", "1");
        datasetTextAttributes.put("cartesian_product", "1;2;4;6;8");
        datasetTextAttributes.put("buying_patterns", "2;3;5;7");
        datasetTextAttributes.put("prediction", "");
        datasetTextAttributes.put("aggregation", "");
    }

    static {
        datasetAttributes = new HashMap<String, String>();
        datasetAttributes.put("customer_list1", "CUSTOMERID;COUNTRY;CONTINENT;AGE;GENDER;CHILDREN;STATUS");
        datasetAttributes.put("customer_list2", "CUSTOMERID;COUNTRY;CONTINENT;AGE;GENDER;CHILDREN;STATUS");
        datasetAttributes.put("customer_list3", "CUSTOMERID;COUNTRY;CONTINENT;AGE;GENDER;CHILDREN;STATUS");
        datasetAttributes.put("customer_list4", "CUSTOMERID;COUNTRY;CONTINENT;AGE;GENDER;CHILDREN;STATUS");
        datasetAttributes.put("deduplication", "CUSTOMERID;COUNTRY;CONTINENT;AGE;GENDER;CHILDREN;STATUS");
        datasetAttributes.put("united_states", "CUSTOMERID;COUNTRY;CONTINENT;AGE;GENDER;CHILDREN;STATUS");
        datasetAttributes.put("europe", "CUSTOMERID;COUNTRY;CONTINENT;AGE;GENDER;CHILDREN;STATUS");
        datasetAttributes.put("union", "CUSTOMERID;COUNTRY;CONTINENT;AGE;GENDER;CHILDREN;STATUS");
        datasetAttributes.put("cloth_items", "CLOTHID;DESCRIPTION");
        datasetAttributes.put("cartesian_product", "CUSTOMERID;COUNTRY;CONTINENT;AGE;GENDER;CHILDREN;STATUS;CLOTHID;DESCRIPTION");
        datasetAttributes.put("buying_patterns", "BUYINGPATTERNID;CLOTHID;COUNTRY;CONTINENT;AGE;GENDER;CHILDREN;STATUS");
        datasetAttributes.put("prediction", "CUSTOMERID;BUYINGPATTERNID;CLOTHID;PROBABILITY");
        datasetAttributes.put("aggregation", "CLOTHID;QUANTITY");
    }

    public static String[] getAttributeMappings(String transformationTag) {
        return datasetIndexingAttributes.get(transformationTag).
                replaceAll("\\[", "").replaceAll("]", "").split(",");

    }

    /**
     * strToCustomerList allows to get a list of Attributes where can get the
     * name, contribution of probability of it.
     *
     * @param str, a tuple from the JavaRDD<String>
     * @return List<Attribute>, the same tuple(String) in List<Attribute> format
     */
    public static List<Attribute> strToCustomerList(String str) {
        List<Attribute> lAtt = new ArrayList();
        String[] list = str.split(";");
        for (int index = 0; index < list.length; index++) {
            lAtt.add(new CustomerAttribute(CustomerAttribute.getAttName(index), list[index], index, CustomerAttribute.getAttProb(index)));
        }
        return lAtt;
    }

    /**
     * strToBuyingPatternList allows to get a list of Attributes where can get
     * the name, contribution of probability of it.
     *
     * @param str, a tuple from the JavaRDD<String>
     * @return List<Attribute>, the same tuple(String) in List<Attribute> format
     */
    public static List<Attribute> strToBuyingPatternList(String str) {
        List<Attribute> lAtt = new ArrayList();
        String[] list = str.split(";");
        for (int index = 0; index < list.length; index++) {
            lAtt.add(new BuyingPatternAttribute(BuyingPatternAttribute.getAttName(index), list[index], index, CustomerAttribute.getAttProb(index)));
        }
        return lAtt;
    }

    public static void runCommandLine(String cmd) {
        try {
            Process p = Runtime.getRuntime().exec(new String[]{"bash", "-c", cmd});
        } catch (IOException ex) {
            Logger.getLogger(Utility.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static String getInputDataset(String dtTag) {
        if (dtTag.equals("cartesian_product")) {
            return "icloth_item";
        } else if (dtTag.equals("prediction")) {
            return "ibuying_pattern";
        }
        return "i" + dtTag;
    }

    public static String getOutputDataset(String dtTag) {
        if (dtTag.contains("buying_patterns")
                || dtTag.contains("cloth_items")
                || dtTag.contains("customer_list")) {
            return dtTag;
        }
        return "o" + dtTag;
    }

    public static String[] toArray(List<String> args) {
        String[] result = new String[args.size()];
        int index = 0;
        for (String a : args) {
            result[index] = a;
            index++;
        }
        return result;
    }

    public static String[] getAttributes(String transformationTag) {
        return datasetAttributes.get(transformationTag).split(SEPARATOR);
    }

    public static int[] getTextAttributes(String datasetTag) {
        String attributes = datasetTextAttributes.get(datasetTag);
        if (!attributes.isEmpty()) {
            return Stream.of(attributes.split(";")).mapToInt(Integer::parseInt).toArray();
        }
        return null;
    }

    private static File[] getRddOutputFiles(String path) {
        File directory = new File(path);
        File[] files = directory.listFiles((dir, name) -> name.startsWith("part-"));
        return files;
    }

    public static void sleep() {
        try {
            Thread.sleep(diskSyncTimeInMilliseconds);
        } catch (InterruptedException ex) {
            Logger.getLogger(Utility.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void makeDir(String workspace) {
        File f = new File(workspace);
        if(!f.exists()){
            f.mkdirs();
        }
    }

}
