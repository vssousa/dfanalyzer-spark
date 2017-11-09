import sys

__author__ = "vitor"
__date__ = "$May 29, 2017 2:44:56 PM$"

def getMasterURL(logFilePath):
#     template
    logTemplate = ": starting org.apache.spark.deploy.worker.Worker, logging to "
    masterTemplate = "Spark Command: "
#    temporary variables
    masterFilePath = None
#    log loop
    for logLine in open(logFilePath,'r').readlines():
        if(logTemplate in logLine):
            masterFilePath = logLine.replace(logTemplate,";").split(";")[1].strip()
#            master loop
            for masterLine in open(masterFilePath,'r').readlines():
                if(masterTemplate in masterLine):
                    return masterLine.split(" ")[-1].strip()
    return None

if __name__ == "__main__":
    if len(sys.argv) > 1:
        masterURL = getMasterURL(sys.argv[1])
        print(masterURL)


            
            
            