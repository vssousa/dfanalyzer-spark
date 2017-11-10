package utility;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author vitor
 */
public class Property {

    private String dataflowTag = "DATAFLOW_ERROR";
    private String resource = "MASTER_ERROR";
    private String workspace = "WORKSPACE_ERROR";
    private boolean debug = false;
    private String restfulURL;
//    raw data access
    private String fastBitBinPath = null;
    private int taskCounter = 1;
    private int subTaskCounter = 1;
    
    public String rdePath;

    public Property() {
        FileReader fr = null;
        try {
            fr = new FileReader(Utility.PROPERTY_FILE_NAME);
            BufferedReader br = new BufferedReader(fr);
            String line;
            String key;
            String value;
            while (br.ready()) {
                line = br.readLine();
                if (line != null && !line.isEmpty()) {
                    String[] slices = line.split("=");
                    if (slices.length == 2) {
                        key = slices[0].toUpperCase();
                        value = slices[1];
                        switch (key) {
                            case "DATAFLOW_TAG":
                                dataflowTag = value;
                                break;
                            case "MASTER":
                                resource = value;
                                break;
                            case "WORKSPACE":
                                workspace = value;
                                rdePath = workspace + "/bin/rde/";
                                break;
                            case "DEBUG":
                                debug = Boolean.parseBoolean(value.toLowerCase());
                                break;
                            case "FASTBIT_PATH":
                                fastBitBinPath = value;
                                break;
                            case "RESTFUL":
                                restfulURL = value;
                                break;
                        }
                    }
                }
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Property.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Property.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public String getDataflowTag() {
        return dataflowTag;
    }

    public String getResource() {
        return resource;
    }

    public String getWorkspace() {
        return workspace;
    }

    public int incrementTask() {
        taskCounter += 1;
        return taskCounter;
    }

    public void clearTask() {
        taskCounter = 1;
    }

    public int incrementSubTask() {
        subTaskCounter += 1;
        return subTaskCounter;
    }

    public void clearSubTask() {
        subTaskCounter = 1;
    }

    public int getTaskID() {
        return taskCounter;
    }

    public int getSubTaskID() {
        return subTaskCounter;
    }

    public String toString() {
        return "DATAFLOW_TAG=" + dataflowTag + "\n"
                + "MASTER=" + resource + "\n"
                + "WORKSPACE=" + workspace + "\n";
    }

    public boolean debug() {
        return debug;
    }

    public String getFastBitBinPath() {
        return fastBitBinPath;
    }

    public String getURL() {
        return restfulURL;
    }
}
