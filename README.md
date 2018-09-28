# SparkViewer

A Java Swing GUI for spark datasets, especially parquet and json files.
Shows structured Datatsets in a comfortable table or tree view and also enables to drill down into substructures.
The GUI can be added to a spark shell, so that you are able to browse the tables that you are working on.

link/embed > table view
link/embed > tree view

## Quick Start
### Prerequisites
You need a SparkInstallation to use this GUI.
The Spark 'lib' directory has to be accesible. 

### start the jar

The jar can directly be started with java.
You can use the prebuild jar of the repository or use your builded jar file.

Example start script for windows spark localmode:
```
SET JAVA_HOME=..path_to..\jdk1.8.0_77
SET SPARK_HOME=..path_to..\spark-2.2.1-bin-hadoop2.7
SET HADOOP_HOME=..path_to..\winutils

SET path=%JAVA_HOME%\bin;%HADOOP_HOME%\bin;
SET CLASSPATH=SparkViewer.jar;%SPARK_HOME%\jars\*
cd ..path_to_sparkviewer_jar..

java -Xmx6000M -Dspark.driver.memory=2g de.equbotic.sparkviewer.SparkViewer (optional parameter: /path/to/sparkdata/directoy/)
```

### start in the sparkshell

You can start the SparkViewer direct in your sparkshell and you are able to browse the datasets you are working with in a comfortable way.
After starting the SparkViewer you need to close the console to quit the sparkshell.
```
:require ..path_to_jar../SparkViewer.jar
import de.equbotic.sparkviewer._
val cmds = new SparkCmds(spark)
SparkMainView.start(cmds)
```
### build the jar

Compile the sources and build the jar:
```
cd ..path_to_src_dir..
javac ..path_to_java_files../*
jar   src.de.equbotic.sparkviewer.*
```

### use with eclipse

Create a new java project in eclipse.
Copy the scr directory to your projrct src directory.
Add the spark jars as external libraries.
Now your project should build.

## View a Dataset

The Table View shows a Spark dataset in one frame with several tabs.
Per default the first 200 rows are shown, which can be changed. 

### Data Table Tab
 
### Tree Tab

### Schema Tab


## then main View (Console)

### menus


## Open Source Infos

### Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.

### Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags). 

### Authors

* **DariusSchneider** - *Initial work* - [SparkViewer](https://github.com/DariusSchneider/SparkViewer)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

### License

This project is licensed under the GNU GPL3 License - see the [LICENSE.md](LICENSE.md) file for details

