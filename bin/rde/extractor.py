import os, sys, fnmatch

print("Input arguments:")
print("sys.argv[1] = extractor tag")

if __name__ == "__main__":
	#input arguments
	cwd = os.getcwd()
	extractorTag = sys.argv[1]
	extractorFileName = extractorTag + ".data"
	filePath = extractorTag + "/"	

	#create directory
	directory = os.path.dirname(filePath)
	if not os.path.exists(directory):
		os.makedirs(directory)

	if("customer_list" in extractorTag or "cloth" in extractorFileName or "buying" in extractorFileName):
		rawFilePath = "../input_dataset/" + extractorTag + ".txt"
		rdd = open(rawFilePath,"r")

		#create file for extracted data
		file = open(filePath + extractorFileName,"w")
		if("customer" in extractorFileName):
			attributes = "FILENAME;CUSTOMERID;COUNTRY;CONTINENT;AGE;GENDER;CHILDREN;STATUS"
		elif("cloth" in extractorFileName):
			attributes = "FILENAME;CLOTHID;DESCRIPTION"
		elif("buying" in extractorFileName):
			attributes = "FILENAME;BUYINGPATTERNID;CLOTHID;COUNTRY;CONTINENT;AGE;GENDER;CHILDREN;STATUS"

		file.write(attributes)
		atts = attributes.split(";")
		for element in rdd:
			values = element.replace("\n","").split(";")
			file.write("\n\"" + cwd + "/" + filePath + extractorFileName + "\"")
			for i in range(len(values)):
				file.write(";\"" + values[i] + "\"")
		file.close()

	else:
		#create file for extracted data
		file = open(filePath + extractorFileName,"w")
		#attributes
		attributes = ""
		if("aggregation" in extractorTag):
			attributes = "FILENAME;CLOTHID;QUANTITY"
		elif("cartesian_product" in extractorTag):
			attributes = "FILENAME;CUSTOMERID;COUNTRY;CONTINENT;AGE;GENDER;CHILDREN;STATUS;CLOTHID;DESCRIPTION"
		elif("deduplication" in extractorTag or "union" in extractorTag or 
			"europe" in extractorTag or "united_states" in extractorTag):
			attributes = "FILENAME;CUSTOMERID;COUNTRY;CONTINENT;AGE;GENDER;CHILDREN;STATUS"
		elif("prediction" in extractorTag):
			attributes = "FILENAME;CUSTOMERID;BUYINGPATTERNID;CLOTHID;PROBABILITY"
		file.write(attributes)
		
		atts = attributes.split(";")
		#search for output files from a RDD	
		for fileName in fnmatch.filter(os.listdir(filePath), 'part-*'):
			os.system("cat %s >> %s" % (filePath + fileName, filePath + extractorFileName))
			rdd = open(filePath + fileName)
			for element in rdd:
				values = element.replace("\n","").split(";")
				print(values)
				file.write("\n\"" + cwd + "/" + filePath + extractorFileName + "\"")
				for i in range(len(values)):
					file.write(";\"" + values[i] + "\"")
		file.close()