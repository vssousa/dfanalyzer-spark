package attribute;

/**
 *
 * @author jean, vitor
 */
public class CustomerAttribute extends Attribute {

    public CustomerAttribute(String name, String value, int index, int p) {
        super(name, value, index, p);
    }

    public static String getAttName(int index) {
        switch (index) {
            case 0:
                return "CUSTOMERID";
            case 1:
                return "COUNTRY";
            case 2:
                return "CONTINENT";
            case 3:
                return "AGE";
            case 4:
                return "GENDER";
            case 5:
                return "CHILDREN";
            case 6:
                return "STATUS";
            default:
                return "";
        }
    }

    /**
     *
     * @param index, index of the attribute
     * @return p, the contribution of the attribute to the probability of the
     * client to buy the cloth
     */
    public static Integer getAttProb(int index) {
        switch (index) {
            case 0:
                return 0;
            case 1:
                return 20;
            case 2:
                return 10;
            case 3:
                return 30;
            case 4:
                return 0;
            case 5:
                return 20;
            case 6:
                return 30;
            default:
                return 0;
        }
    }
}
