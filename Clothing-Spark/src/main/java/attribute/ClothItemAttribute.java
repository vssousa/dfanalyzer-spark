package attribute;

/**
 *
 * @author jean, vitor
 */
public enum ClothItemAttribute {
    CLOTHID(0),DESCRIPTION(1);
    
    public int index;
    ClothItemAttribute(int index){
        this.index = index;
    }
    
    public int getIndex(){
        return index;
    }

}
