package pde;

/**
 *
 * @author vitor
 */
public class Task {

    private final String dataflowTag;
    private final String transformationTag;
    private final int ID;
    private String status;
    private final String workspace;
    private final String resource;

    public Task(String dfTag, String dtTag, int ID, String status,
            String workspace, String resource) {
        this.dataflowTag = dfTag;
        this.transformationTag = dtTag;
        this.ID = ID;
        this.status = status;
        this.workspace = workspace;
        this.resource = resource;
    }
    
    public void setStatus(String status){
        this.status = status;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("task(").append(dataflowTag).append(",")
                .append(transformationTag).append(",")
                .append(ID).append(", ,").append(status).append(",")
                .append(workspace).append(",")
                .append(resource).append(")");
        return str.toString();
    }

}
