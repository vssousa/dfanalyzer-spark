package attribute;

/**
 *
 * @author jean, vitor
 */
public abstract class Attribute {

    private String name;
    private String value;
    private int index;
    private int position;

    public Attribute(String name, String value, int index, int position) {
        this.name = name;
        this.value = value;
        this.index = index;
        this.position = position;
    }

    public int getIndex() {
        return index;
    }

    public int getPosition() {
        return position;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return name;
    }

    public boolean equals(String attributeName) {
        return this.name.equals(attributeName);
    }

}
