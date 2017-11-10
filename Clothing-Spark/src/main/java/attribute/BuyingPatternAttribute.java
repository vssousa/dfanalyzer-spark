package attribute;

/**
 *
 * @author jean, vitor
 */
public class BuyingPatternAttribute extends Attribute {

    public BuyingPatternAttribute(String name, String value, int index, int p) {
        super(name, value, index, p);
    }

    public static String getAttName(int index) {
        switch (index) {
            case 0:
                return "BUYINGPATTERNID";
            case 1:
                return "CLOTHID";
            case 2:
                return "COUNTRY";
            case 3:
                return "CONTINENT";
            case 4:
                return "AGE";
            case 5:
                return "GENDER";
            case 6:
                return "CHILDREN";
            case 7:
                return "STATUS";
            default:
                return "";
        }
    }
}
