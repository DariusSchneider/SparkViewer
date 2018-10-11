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

import javax.swing.table.AbstractTableModel;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;

/**
 * The Table Model of the Row Array.<br>
 * Implements the methods of AbstractTableModel.  
 * 
 * @author DariusSchneider, luca@equbotic.com
 */
public class DfTableModel extends AbstractTableModel {
			private static final long serialVersionUID = 1L;
			
			private Row[]          rows      = null;
	        private StructType     schema    = null;
	        private String[]       fldnames  = null;
	        private String         title     = null;
            private Dataset<Row>   dataset   = null;
            private String         tabname   = null;
	        
            /**
             * Creates the DfTableModel for the given parameters.
             * 
	       	 * @param rrr - the Row array.
	    	 * @param schema - the schema for a Row.
             * @param titl - the title of the model.
             * @param ds - optional, the Dataset behind the Rows.  
             * @param tabname - the tabname of the dataset in the catalog.
             */
	        public DfTableModel(Row[] rr, StructType schem, String titl, Dataset<Row> ds, String tabnam) {	        
	        	rows   = rr;
	        	schema = schem;
	        	fldnames = schema.fieldNames();
	        	title    = titl;
	        	dataset  = ds;
	        	tabname  = tabnam;
	        }

	        /**
	         * getter for the dataset.
	         * @return this.dataset
	         */
	        public Dataset<Row> getDataFrame() {
	        	return dataset;
	        }
	        /**
	         * getter for the tablename.
	         * @return this.tabname.
	         */
	        public String getTableName() {
	        	return tabname;
	        }
	        /**
	         * getter for the fieldnames.
	         * @return this.fldnames
	         */
	        public String[] getFieldNames() {
	        	return fldnames;
	        }
	        /**
	         * getter for the title.
	         * @return this.title
	         */
	        public String getTitle() {
	        	return title;
	        }
	        /**
	         * getter for the title without number of rows.
	         * @return this.title without '(..)'
	         */
	        public String getTitleBas() {
	        	int ii = title.indexOf('(');
        	    return (ii<0) ? title : title.substring(0, title.indexOf('('));
	        }
	        /**
	         * getter for the number of columns.
	         * @return fldnames.length
	         * @see TableModel
	         */
		    public int getColumnCount() {
		        return fldnames.length;
		    }
	        /**
	         * getter for the number of rows.
	         * @return rows.length
	         * @see TableModel
	         */
		    public int getRowCount() {
		        return rows.length;
		    }
	        /**
	         * getter for the name of a column.
	         * @param col - the index of the requested column.
	         * @return fldnames[col]
	         * @see TableModel
	         */
		    public String getColumnName(int col) {
		        return fldnames[col];
		    }
	        /**
	         * getter for the value of a cell used for gui.
	         * @param roe - the index of the requested row.
	         * @param col - the index of the requested column.
	         * @return Object/String of the cell
	         * @see TableModel
	         */
		    public Object getValueAt(int row, int col) {
		    	try {
		    	  Object oo   = rows[row].get(col);
		    	  String ostr = oo.toString();
		    	  if (ostr.startsWith("WrappedArray"))
		    		  return ostr.substring(12); 
		    	  else 
		    		  return oo;
		    	  
		    	} catch (Exception ee) {
		    		return null;
		    	}
		    }
	        /**
	         * getter for the object behind a cell.
	         * @param roe - the index of the requested row.
	         * @param col - the index of the requested column.
	         * @return Object of the cell
	         */
		    public Object getObjectAt(int row, int col) {
		    	try {
		    	  return rows[row].get(col);
		    	} catch (Exception ee) {
		    		return null;
		    	}
		    }
	        /**
	         * getter for the class of a column.
	         * @param c - the index of the requested column.
	         * @return an object representing the column.
	         * @see TableModel
	         */
		    public Class<?> getColumnClass(int c) {
		    //	schema.fields()[c].dataType()  //TODO set correct Types if null
		    //TODO Datetime ist not correctly shown
		    	Object oo = null;
		    	if      (rows.length > 1) oo = getValueAt(1, c);
		    	else if (rows.length > 0) oo = getValueAt(0, c);
		    	
		    	if (oo == null) return String.class;
		    	else            return oo.getClass();
		    }
}		    