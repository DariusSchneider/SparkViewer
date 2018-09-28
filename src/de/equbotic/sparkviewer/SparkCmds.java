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
		// TODO run multiple commands

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
			if (pars.length < 3 || pars[2].isEmpty())
				switch (cmd) {
				case "openfile":
					par2 = par1.substring(par1.lastIndexOf('/') + 1).trim();
					break;
				default:
					throw new IllegalArgumentException("Invalid command: " + cmd);
				}
			else
				par2 = pars[2].trim();

			// "openfile; FileName; HiveTempName"
			// "execsql; HiveTempName; sqlStr"
			// "writetable; FileName; HiveTempName (makes reread)"
			// "writecsv; FileName; HiveTempName"

			switch (cmd) {
			case "openfile":
				Dataset<Row> dataset1 = spark.read().parquet(par1);
				dataset1.createOrReplaceTempView(par2);
				return par2;
			case "execsql":
				Dataset<Row> dataset2 = spark.sql(par1);
				dataset2.createOrReplaceTempView(par2);
				return par1;
			case "writetable":
				spark.table(par2).write().mode("overwrite").parquet(par1);
				Dataset<Row> dataset3 = spark.read().parquet(par1);
				dataset3.createOrReplaceTempView(par2);
				return par2;
			case "writecsv":
				spark.table(par2).repartition(1).write().mode("overwrite").option("header", "true")
						.option("delimiter", ";").csv(par1);
				return null;
			default:
				throw new IllegalArgumentException("Invalid command: " + cmd);
			}

		} catch (Exception ee) {
			throw new IllegalArgumentException("Invalid command string: " + cmdStr + "\n" + ee.getMessage());
		}
	}

}
