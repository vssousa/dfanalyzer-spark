package function;

import org.apache.spark.api.java.function.Function;

/**
 *
 * @author vitor
 */
public class FilterFunction implements Function<String, Boolean> {
    
    private Integer attributeIndex;
    private String filterCondition;
    
    public FilterFunction(int attributeIndex, String filterCondition){
        this.attributeIndex = attributeIndex;
        this.filterCondition = filterCondition;
    }

    @Override
    public Boolean call(String str) throws Exception {
        return str.split(";")[attributeIndex].toLowerCase().contains(filterCondition);
    }

}
