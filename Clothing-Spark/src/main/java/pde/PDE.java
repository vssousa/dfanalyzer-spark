package pde;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import java.util.logging.Level;
import java.util.logging.Logger;
import utility.Utility;

/**
 *
 * @author vitor
 */
public class PDE {
    
    private final String dfaURL;
    private Task task;
    private StringBuilder collections;
    private StringBuilder depedencies;
    
    public PDE(String url){
        this.dfaURL = "http://" + url + ":22000/pde/task";
        this.collections = new StringBuilder();
        this.depedencies = new StringBuilder();
    }
    
    public void task(String dfTag, String dtTag, int ID, String status, 
            String workspace, String resource){
        this.task = new Task(dfTag, dtTag, ID, status, workspace, resource);
    }
    
    public void collection(String dsTag, String collection){
        if(!this.collections.toString().isEmpty()){
            this.collections.append("\n");
        }
        this.collections.append("collection(").append(dsTag).append(",").append(collection).append(")");
    }
    
    public void dependency(String transformations, String taskIDs){
        if(!this.depedencies.toString().isEmpty()){
            this.depedencies.append("\n");
        }
        this.depedencies.append("dependency(").append(transformations).append(",").append(taskIDs).append(")");
    }
    
    public void sendRequest() {
        String message = "";
        if(this.task != null){
            message += this.task.toString();
            if(!this.collections.toString().isEmpty()){
                message += "\n" + this.collections;
            }
            if(!this.depedencies.toString().isEmpty()){
                message += "\n" + this.depedencies;
            }
        }
        
        try {
            HttpResponse<String> response = Unirest.post(this.dfaURL)
                    .header("cache-control", "no-cache")
                    .header("postman-token", "eece8f30-47e0-c778-c419-38536c595261")
                    .body(message)
                    .asString();
            clearCollections();
        } catch (UnirestException ex) {
            Logger.getLogger(Utility.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void clearMessage(){
        clearTask();
        clearCollections();
        clearDependencies();
    }
    
    public void clearTask(){
        this.task = null;
    }
    
    public void clearCollections(){
        this.collections = new StringBuilder();
    }
    
    public void clearDependencies(){
        this.depedencies = new StringBuilder();
    }

    public void changeTaskStatus(String status) {
        this.task.setStatus(status);
    }
    
}
