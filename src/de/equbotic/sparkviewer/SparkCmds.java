/*
SparkViewer
Copyright (C) 2018  equbotic

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

import javax.swing.JTextArea;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class SparkCmds {
	private SparkSession spark = null;

	public SparkCmds(SparkSession sprk) {
		spark = sprk;
	}

	public SparkSession getSpark() {
		return spark;
	}
	
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

			// "openfile;  FileName; HiveTempName"
			// "execsql;   HiveTempName; sqlStr"
			// "writepq;   FileName; HiveTempName (makes reread)"
			// "writecsv;  FileName; HiveTempName"
			// "writejson; FileName; HiveTempName"
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

	public String open (String filename) {
		return open(filename, null);
	}
	public String open (String filename, String tabname) {
		if (tabname == null)
			tabname = filename.substring(filename.replace('\\','/').lastIndexOf('/') + 1).trim();
		
		Dataset<Row> dataset1 = spark.read().parquet(filename);
	    dataset1.createOrReplaceTempView(tabname);
	    return tabname;
	}
	public Dataset<Row> execsql (String sqlstr, String tabname) {				
		Dataset<Row> dataset1 = spark.sql(sqlstr);
	    dataset1.createOrReplaceTempView(tabname);
	    return dataset1;
	}
	public void writepq  (String filename, Dataset<Row> table) {				
		table.write().mode("overwrite").parquet(filename);
	}	
	public Dataset<Row> writepq  (String filename, String tabname) {				
		spark.table(tabname).write().mode("overwrite").parquet(filename);
		Dataset<Row> dataset1 = spark.read().parquet(filename);
		dataset1.createOrReplaceTempView(tabname);
	    return dataset1;
	}
	public void writecsv (String filename, String tabname) {
		writecsv (filename, spark.table(tabname));
	}
	public void writecsv (String filename, Dataset<Row> table) {				
		table.repartition(1).write().mode("overwrite").option("header", "true").option("delimiter", ";").csv(filename);
	}
	public void writejson (String filename, String tabname) {
		writejson (filename, spark.table(tabname));
	}
	public void writejson(String filename, Dataset<Row> table) {				
		table.repartition(1).write().mode("overwrite").option("multiLine", "true").json(filename);
	}

}
