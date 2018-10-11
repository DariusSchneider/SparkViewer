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

import java.io.File;
import javax.swing.JTextArea;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkCmds {
	/**
	 * the SparkSession of this SparkCmds object.<br>
	 */
	private SparkSession spark = null;
	/**
	 * Sets the SparkSession of this SparkCmds object.<br>
	 * Needs to be set before anything can be done.
	 * @param sprk
	 */
	public SparkCmds(SparkSession sprk) {
		spark = sprk;
	}
	/**
	 * getter for SparkSession of this object. 
	 * @return this.SparkSession
	 */
	public SparkSession getSpark() {
		return spark;
	}
	
    /**	
     * Executes the commands that are in the cmdStr.<br>
     * The text is interpreted until the first line starting with '='.<br>
     * A command ends with a line starting with '-'.
     * This command is seperated and exeuted with 'execCmd'. 
     * 
     * @param cmdStr - String with commands
     * @param jtxt - optional, a JTextArea where some logging will be added
     * @return the last table name of execution, if exists.
     */
	public String execTxt(String cmdStr, JTextArea jtxt) {
		String[] lnarr = cmdStr.split("\n");
		String cmd = "";
		String ret = "";
		for (String ln : lnarr) {
			if (!ln.isEmpty()) {
				if (ln.startsWith("-") || ln.startsWith("=")) { // cmd ends
					if (!cmd.isEmpty()) {
						if (jtxt != null)
							jtxt.setText(jtxt.getText() + "\n" + cmd);
						ret = execCmd(cmd);
						if (jtxt != null)
							jtxt.setText(jtxt.getText() + " --- end");
						cmd = "";
					}
				} else
					cmd = cmd + ln + " ";

				if (ln.startsWith("=")) { // cmd ends
					break;
				}
			}
		}
		return ret;
	}
	/**
	 * Executes the given command.<br>
	 * The cmdStr is interpreted and the executed:<br>
	 * openfile;  FileName; HiveTempName <br>
	 * execsql;   HiveTempName; sqlStr <br>
	 * writepq;   FileName; HiveTempName (makes reread) <br>
	 * writecsv;  FileName; HiveTempName <br>
	 * writejson; FileName; HiveTempName <br>
	 * 
	 * @param cmdStr - to be executed
	 * @return a catalog table name if possible 
	 */
	public String execCmd(String cmdStr) {
		try {
			String[] pars = cmdStr.trim().split(";");

			// "openfile; FileName; HiveTempName"
			// "execsql; sqlStr; HiveTempName"
			// "writetable; FileName; HiveTempName (makes reread)"
			// "writecsv; FileName; HiveTempName"

			String cmd = pars[0].trim();
			String par1 = pars[1].trim();

			String par2 = null;
			if (pars.length > 2)
				par2 = pars[2].trim();

			switch (cmd) {
			case "open":
				return open(par1, par2);
			case "execsql":
				execsql(par1, par2);
				return par2;
			case "writepq":
				writepq(par1, par2);
				return par2;
			case "writecsv":
				writecsv(par1, par2);
				return null;
			case "writejson":
				writejson(par1, par2);
				return null;
			default:
				throw new IllegalArgumentException("Invalid command: " + cmd);
			}

		} catch (Exception ee) {
			throw new IllegalArgumentException("Invalid command string: " + cmdStr + "\n" + ee.getMessage());
		}
	}

	/**
	 * Infers the type of the given filename.<br>
	 * At first the file himself is checked for the type extension.<br>
	 * Next the filename is treated as directory and the files are checked for a type extension.<br> 
	 * As default, 'parquet' is returned.
	 * 
	 * @param fname - the filename to infer.
	 * @return the type as a String (parquet, json, csv)
	 */
	public String inferType(String fname) {
		if (fname == null || fname.isEmpty()) return "parquet";
		String ff = fname.trim().toLowerCase();
		if (ff.lastIndexOf(".parquet") > 0) return "parquet"; 
		if (ff.lastIndexOf(".json")    > 0) return "json"; 
		if (ff.lastIndexOf(".csv")     > 0) return "csv";
		
		File dir = new File(fname.trim());
		if (dir.isFile())  return "parquet"; 

        File[] files = dir.listFiles();
        for (File fil : files) {
        	String filnam = fil.getName(); 
    		if (filnam.lastIndexOf(".parquet") > 0) return "parquet"; 
    		if (filnam.lastIndexOf(".json")    > 0) return "json"; 
    		if (filnam.lastIndexOf(".csv")     > 0) return "csv";
        }
		return "parquet";
	}
	/**
	 * Returns the name of the given full filename without directoy and extension,
	 * @param fname - the full filename
	 * @return tablename 
	 */
	public String getTableName(String fname) {
		if (fname == null || fname.isEmpty()) return fname;
		String ff = fname.replace('\\','/').trim();
		if (ff.isEmpty()) return ff;		
		int len   = ff.length();
 		ff = (ff.charAt(len-1) == '/') ? ff.substring(0, len-2) : ff;  // remove trailing slash

		int lasti = ff.lastIndexOf('/');
		ff = (lasti < 0) ? ff : ff.substring(ff.lastIndexOf('/') + 1).trim();  //read filename after slash

		int firstp = ff.indexOf('.');
		ff = (firstp < 0) ? ff : ff.substring(0, firstp).trim();  //read name before point
		if (ff.length()>101) ff = ff.substring(0, 100); //cut to long filename
		ff = ff.replace('-','_');
		return ff;
	}
	/**
	 * Opens the given filename with the default tablename.
	 * @param filename - the full filename
	 * @return the tablename
	 * @see open
	 */
	public String open (String filename) {
		return open(filename, null);
	}
	/**
	 * Opens the given filename and sets the catalog tablename.<br>
	 * If no tablename is given, the default tablename ist used ('getTableName').
	 * @param filename - the full filename
	 * @param tabname - optional 
	 * @return the tablename
	 */
	public String open (String filename, String tabname) {
		if (tabname == null) tabname = getTableName(filename);
		String formt = inferType(filename);
		Dataset<Row> dataset1 = spark.read().
				                option("header","true").option("inferschema", "true").option("sep", ";").
				              //option("multiLine", "true").  //doesnt work?
				                format(formt).load(filename);
	    dataset1.createOrReplaceTempView(tabname); 
	    return tabname;
	}
	/**
	 * Executes the given sql string and sets the tablename in the catalog.  
	 * @param sqlstr
	 * @param tabname
	 * @return the dataset produced
	 */
	public Dataset<Row> execsql (String sqlstr, String tabname) {				
		Dataset<Row> dataset1 = spark.sql(sqlstr);
	    dataset1.createOrReplaceTempView(tabname);
	    return dataset1;
	}
	/**
	 * Writes a given Dataset (table) as parquetfile to 'filename'.
	 * @param filename - to be written to.
	 * @param table - as Dataset
	 */
	public void writepq  (String filename, Dataset<Row> table) {				
		table.write().mode("overwrite").parquet(filename);
	}	
	/**
	 * Writes a given Dataset (catalog tabname) as parquetfile to 'filename' and rereads the file in catalog tabname.
	 * @param filename
	 * @param tabname
	 * @return the rereaded dataset 
	 */
	public Dataset<Row> writepq  (String filename, String tabname) {				
		spark.table(tabname).write().mode("overwrite").parquet(filename);
		Dataset<Row> dataset1 = spark.read().parquet(filename);
		dataset1.createOrReplaceTempView(tabname);
	    return dataset1;
	}
	/**
	 * Writes a given Dataset (catalog tabname) as csv file to 'filename'.
	 * @param filename
	 * @param tabname
	 */
	public void writecsv (String filename, String tabname) {
		writecsv (filename, spark.table(tabname));
	}
	/**
	 * Writes a given Dataset (table) as csv file to 'filename'.
	 * @param filename 
	 * @param table - the dataset to be written
	 */
	public void writecsv (String filename, Dataset<Row> table) {				
		table.repartition(1).write().mode("overwrite").option("header", "true").option("delimiter", ";").csv(filename);
	}
	/**
	 * Writes a given Dataset (catalog tabname) as json file to 'filename'.
	 * @param filename
	 * @param tabname
	 */
	public void writejson (String filename, String tabname) {
		writejson (filename, spark.table(tabname));
	}
	/**
	 * Writes a given Dataset (table) as json file to 'filename'.
	 * @param filename 
	 * @param table - the dataset to be written
	 */
	public void writejson(String filename, Dataset<Row> table) {				
		table.repartition(1).write().mode("overwrite").option("multiLine", "true").json(filename);
	}

}
