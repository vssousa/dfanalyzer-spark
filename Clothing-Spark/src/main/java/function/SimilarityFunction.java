package function;

import attribute.Attribute;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.List;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import utility.Utility;

/**
 *
 * @author vitor
 */
public class SimilarityFunction implements Function<Tuple2<String,String>, String> {

    /**
     * Compare all the Attributes of the costumer tuples and the buying pattern
     * tuples to get the client's buying pattern
     *
     * @complexity n * m where n is the size of the cust tuples and m is the
     * size of the bp list TODO minimize
     * @return
     */
    public static double compareCustomerToBuyingPattern(List<Attribute> customerAttributes, List<Attribute> buyingPatternAttributes) {
        double similarity = 0.00;
        for (Attribute customerAttribute : customerAttributes) {
            for (Attribute buyingPatternAttribute : buyingPatternAttributes) {
                if (customerAttribute.toString().equals(buyingPatternAttribute.toString())) {
                    if (customerAttribute.equals("COUNTRY")
                            && customerAttribute.getValue().equals(buyingPatternAttribute.getValue())) {
                        similarity += 0.70;
                    } else if (customerAttribute.equals("CONTINENT")
                            && customerAttribute.getValue().equals(buyingPatternAttribute.getValue())) {
                        similarity += 0.60;
                    } else if (customerAttribute.equals("AGE")
                            && Integer.parseInt(customerAttribute.getValue()) - 7 <= Integer.parseInt(buyingPatternAttribute.getValue())
                            && Integer.parseInt(customerAttribute.getValue()) + 7 >= Integer.parseInt(buyingPatternAttribute.getValue())) {
                        similarity += 0.40;
                    } else if (customerAttribute.equals("GENDER")
                            && customerAttribute.getValue().equals(buyingPatternAttribute.getValue())) {
                        similarity += 0.20;
                    } else if (customerAttribute.equals("CHILDREN")
                            && customerAttribute.getValue().equals(buyingPatternAttribute.getValue())) {
                        similarity += 0.30;
                    } else if (customerAttribute.equals("STATUS")
                            && customerAttribute.getValue().equals(buyingPatternAttribute.getValue())) {
                        similarity += 0.45;
                    }
                }
            }
        }
        return similarity;
    }

    @Override
    public String call(Tuple2<String, String> t) throws Exception {
        String customer = t._1().split(Utility.SEPARATOR)[0];
        
        String[] bpSplit = t._2().split(Utility.SEPARATOR);
        String buyingPattern = bpSplit[0] + Utility.SEPARATOR + bpSplit[1];

        List<Attribute> customerAtt = Utility.strToCustomerList(t._1());
        List<Attribute> buyingPatternAtt = Utility.strToBuyingPatternList(t._2());

        double similarity = compareCustomerToBuyingPattern(customerAtt, buyingPatternAtt); // calculates probability

        if (similarity > 1.00) {
            similarity = 1.00;
        }
        
        NumberFormat formatter = new DecimalFormat("#0.00");     
        return customer + Utility.SEPARATOR
                + buyingPattern + Utility.SEPARATOR
                + formatter.format(similarity);
    }
}
