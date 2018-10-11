/*
SparkViewer
Copyright (C) 2018  DariusSchneider, luca@equbotic.com

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package de.equbotic.sparkviewer;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;

/**
 * This class implements the main method to start SparkViewer standalone. 
 * 
 * @author DariusSchneider, luca@equbotic.com
 */
public class SparkViewer {

	/**
	 * The main method of SparkViewer.<br>
	 * Starts the standalone application.  
	 * 
	 * @param args - 1 parameter possible: the default path, which is setted as 'lastDir'.
	 * @throws AnalysisException
	 */
	public static void main(String[] args) throws AnalysisException {
		
		SparkSession spark = SparkSession.builder().appName("SparkViewer").master("local[*]").getOrCreate();
	    spark.sparkContext().setLogLevel("WARN");
	                  
	    if (args.length > 0) {
	      SparkMainView.setLastDir(args[0]);       
	    }
	    
	    SparkMainView.start(new SparkCmds(spark));
	}

}
