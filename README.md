# SparkViewer
A Java Swing GUI for spark datasets, especially parquet and json files.
Shows structured Datatsets in a comfortable table or tree view and also enables to drill down into substructures.
The GUI can be added to a spark shell, so that you are able to browse the tables that you are working on.
SparkViewer uses the hive catalog to access the tables. 
So a temporary Spark Dataset can be made accessible by adding it to the catalog (dataset.createOrReplaceTempView(tabname)).

![SparkViewer0](SparkViewer.png)

## Quick Start
### Prerequisites
You need a Spark Installation to use this GUI.
The Spark 'jars' directory has to be accessible. 

### start in the sparkshell
You can start the SparkViewer direct in your sparkshell and you are able to browse the datasets you are working with in a comfortable way. 
After starting the SparkViewer you need to close the SparkViewer console to quit the sparkshell.
```
:require ..path_to_jar../SparkViewer.jar
import de.equbotic.sparkviewer._
val cmds = new SparkCmds(spark)
SparkMainView.start(cmds)
```

### start the jar
The jar can directly be started with java.
You can use the prebuild jar of the repository or use your builded jar file.

Example start script for windows spark localmode:
```
cd ..path_to_sparkviewer_jar..
SET CLASSPATH=SparkViewer.jar;..path_to../spark-2.x.x-bin-hadoop2.7/jars/*

java -Xmx6000M -Dspark.driver.memory=2g de.equbotic.sparkviewer.SparkViewer (optional parameter: /path/to/sparkdata/directoy/)
```

### build the jar
Compile the sources and build the jar:
```
cd ..path_to_SparkViewer../src
javac de/equbotic/sparkviewer/*.java -cp .;..path_to../spark-2.x.x-bin-hadoop2.7/jars/* 
jar cvf sparkViewer.jar de/equbotic/sparkviewer/*.class de/equbotic/sparkviewer/*.md 
```
### use with eclipse
```
Create a new java project in eclipse.
Copy the scr directory to your projrct src directory.
Add the spark jars as external libraries.
Now your project should build.
```
## Table View for a Dataset
The Table View shows a Spark dataset in one frame with several tabs.
Per default the first 200 rows are shown, which can be changed. 

### Data Table Tab
Show the first n (default=200) rows in a table view.
Functions:

* moving the mouse over cells shows the complete value in a hint box
* double click on a structured field, opens a new Table View with the details of the field.
* right click opens a popup menu with actions on the dataset (not available for structured fields)

Popup Menu:

* filter - Opens a dialog where a filter definition can be entered (sql syntax, 'column =  123').  
           After confiming, a new Table View is created.
* limit -  Opens a dialog where a new limit for the table size can be defined (default = 200).  
           After confiming, a new Table View is created.
* distinct - Opens a dialog where a column for grouping can be entered.   
             After confiming, a new Table View is created with the distinct values of the column and the count of rows.
* copy_headers - Copies the column headers (tab seperated) to the clipboard (you can paste this to excel).
                 The rows can be marked with the cursor and copied with ctrl-v (excel needs to accept '.' as decimal point).
* writepq_and_load - Opens a new file dialog.
              After confiming, the dataset is written to the directory and rereaded with the filename as hive table name.
* writecsv -  Opens a new file dialog.
              After confiming, the dataset is written to the directory as csv-file. The csv-file is separated with ';'.
* writejson - Opens a new file dialog.
              After confiming, the dataset is written to the directory as json-file.

### Tree Tab
Shows the Rows in a Json like Tree-View, where you can drill down into subtrees.

### Schema Tab
Shows the Schema of the table.


## the Console (Main View)
The Main View is the Console of the applikation. When you close this Window the application is closed

### the Console Area
The console consists of 3 areas separated by a line with '=':

1. commands - here you can place commands that can be interepreted and executed.
             Multiple commands can be entered. They have to be separated by a line starting with '-'.
2. infos - here a short description is placed and the commands syntax is listed:
		open;       	FileName; 	TableName (infer type)
		execsql;    	SqlStr;   	TableName
		writepq;    	FileName; 	TableName (makes reread)
		writecsv;   	FileName; 	TableName
		writejson;  	FileName; 	TableName
3. log - here logging of the actions ist added

### the Menus

#### EXEC/Open
* Exec Command - executes everything in the command area.
* Exec Marked - executes the markes text.
* Open File (infer type) - opens a file open dialogbox.
           After confiming, the file/directory is interpreted to infer the type and a new Table View is created.
           
#### Tables
* Refresh Tablelist - refreshes the tablelist that is shown in this menu from the hive catalog.
* Prefix: - opens a dialogbox whre a prefix for the tablelist can be entered.
* ..table.. - the tables of the hive catalog are listed as menu entries and can be opened by this entry.

#### Help
* ReadMe - shows this readme in a text window.
* About - shows about information.
           
## Open Source Infos
### Contributing
Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.

### Versioning
We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags). 

### Authors
* **DariusSchneider** - *Initial work* - [SparkViewer](https://github.com/DariusSchneider/SparkViewer)

See also the list of [contributors](https://github.com/DariusSchneider/SparkViewer/contributors) who participated in this project.

### License
This project is licensed under the GNU GPL3 License - see the [LICENSE.md](LICENSE.md) file for details

