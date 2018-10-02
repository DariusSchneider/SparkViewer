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

import javax.swing.table.AbstractTableModel;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;

public class DfTableModel extends AbstractTableModel {
	
	        private Row[]      rows      = null;
	        private StructType schema    = null;
	        String[]           fldnames  = null;
	        String             title     = null;
            Dataset<Row>       dataset   = null;
            String             tabname   = null;
	        
	        public DfTableModel(Row[] rr, StructType schem, String titl, Dataset<Row> ds, String tabnam) {	        
	        	rows   = rr;
	        	schema = schem;
	        	fldnames = schema.fieldNames();
	        	title    = titl;
	        	dataset  = ds;
	        	tabname  = tabnam;
	        }
		//  private String[] columnNames = ...//same as before...
		//  private Object[][] data = ...//same as before...

	        public Dataset<Row> getDataFrame() {
	        	return dataset;
	        }

	        public String getTableName() {
	        	return tabname;
	        }

	        public String[] getFieldNames() {
	        	return fldnames;
	        }
	        
	        public String getTitle() {
	        	return title;
	        }
	        public String getTitleBas() {
        	    return title.substring(0, title.indexOf('('));
	        }
	        
		    public int getColumnCount() {
		        return fldnames.length;
		    }

		    public int getRowCount() {
		        return rows.length;
		    }

		    public String getColumnName(int col) {
		        return fldnames[col];
		    }

		    public Object getValueAt(int row, int col) {
		    	try {
		    	  return rows[row].get(col);
		    	} catch (Exception ee) {
		    		return null;
		    	}
		    }

		    public Class getColumnClass(int c) {
		    //	schema.fields()[c].dataType()  //TODO set correct Types if null
		    //TODO Datetime ist not correctly shown
		    	Object oo = null;
		    	if      (rows.length > 1) oo = getValueAt(1, c);
		    	else if (rows.length > 0) oo = getValueAt(0, c);
		    	
		    	if (oo == null) return String.class;
		    	else            return oo.getClass();
		    }
}		    