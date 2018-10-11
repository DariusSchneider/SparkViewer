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

import java.awt.Dimension;
import java.awt.Toolkit;
import java.awt.datatransfer.StringSelection;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.io.InputStream;
import java.util.List;
import javax.swing.*;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import scala.collection.mutable.WrappedArray;

/**
 * This class handles all gui components of SparkViewer. 
 * 
 * @author DariusSchneider, luca@equbotic.com
 */
public class SparkMainView {
	/** 
	 * default limit size of the table model. 
	 */
	public static int limit0 = 200;
	/** 
	 * Mouse Adapter for the Table-View. <br>
	 * Double click a Cell with structured data to open new TableView for the data.
	 */
	private static MouseAdapter openClick = new MouseAdapter() {
		public void mouseClicked(MouseEvent e) {
			if (e.getClickCount() == 2) {
				JTable tabl = (JTable) e.getSource();
				int rr = tabl.convertRowIndexToModel(tabl.getSelectedRow());
				int cc = tabl.convertColumnIndexToModel(tabl.getSelectedColumn());

				DfTableModel mymod = (DfTableModel) tabl.getModel();
				Object cel = mymod.getObjectAt(rr, cc);

				if (cel != null && (cel instanceof WrappedArray<?> || cel instanceof GenericRowWithSchema)) {

					String colnam = mymod.getColumnName(cc);
					String titl = mymod.getTitleBas() + " - " + colnam + " of row " + (rr + 1);

					if (cel instanceof WrappedArray<?>) {
						@SuppressWarnings("unchecked")
						WrappedArray<Row> wa = (WrappedArray<Row>) cel;
						Row[] oo = (Row[]) wa.array();
						StructType schema = oo[0].schema();

						titl = titl + " (" + oo.length + ")";
						performopenArr(oo, schema, titl, null, null);
					} else if (cel instanceof GenericRowWithSchema) {
						Row ro = ((GenericRowWithSchema) cel);
						Row[] oo = new Row[] { ro };
						StructType schema = ro.schema();

						titl = titl + " (1)";
						performopenArr(oo, schema, titl, null, null);
					}
				}
			}
		}
	};

	/** 
	 * ActionListener for PopUp-Menu a table view, that  shows a dataset.<br>
	 * Handled actons are: <br>
	 * filter the dataset, <br>
	 * set the limit size of the shown rows, <br> 
	 * show distinct values of a column, <br>
	 * write the dataset as parquet file and reload the table, <br>
	 * write the dataset as csv or json file.<br>
	 * <br>
	 * For the action a dialog box ist shown.
	 */
	static class TabActionListener implements ActionListener {
		JTable dftable = null;

		public TabActionListener(JTable tabl) {
			dftable = tabl;
		}

		@Override
		public void actionPerformed(ActionEvent e) {

			String name = e.getActionCommand();
			DfTableModel dfTabMod = (DfTableModel) dftable.getModel();
			String titlBas = dfTabMod.getTitleBas();

			int cc = dftable.getSelectedColumn();
			String colnam = (cc < 0) ? "?" : dftable.getColumnName(cc);

			if (name.startsWith("copy_headers")) { //just copy the headers
                String[] flds = dfTabMod.getFieldNames();
				String fldstr = "";
				for (String fld : flds) fldstr = fldstr + (fldstr.isEmpty() ? "" : "\t") + fld;
				Toolkit.getDefaultToolkit().getSystemClipboard().setContents(new StringSelection(fldstr), null);
				return;
			}
			
			String intext = null;
			if (name.startsWith("write")) {
				JFileChooser jFileChooser = new JFileChooser(lastDir);
				             jFileChooser.setDialogTitle(name + ": select name"); 
				int result = jFileChooser.showSaveDialog(new JFrame());
				if (result == JFileChooser.APPROVE_OPTION) {
					File tosave = jFileChooser.getSelectedFile();
					intext = tosave.getAbsolutePath();
				}
			} else
				intext = JOptionPane.showInputDialog(name, colnam);
			
			
			if (intext != null) {

				Dataset<Row> ds = dfTabMod.getDataFrame();

				switch (name) {
				case "filter":
					Dataset<Row> dsfil = ds.filter(intext);
					performopen(dsfil, null, titlBas + " > filter:" + intext, limit0);
					break;
				case "limit":
					performopen(ds, null, titlBas, Integer.parseInt(intext));
					break;
				case "distinct":
					Dataset<Row> dsdis = ds.groupBy(intext).count().sort(intext);
					performopen(dsdis, null, titlBas + " > distinct:" + intext, limit0);
					break;
				case "writepq_and_load":
					sparkCmd.writepq(intext, ds);
					String nam = sparkCmd.open (intext);
					Dataset<Row> dataset = sparkCmd.getSpark().table(nam);
					performopen(dataset, nam, nam, limit0);
					break;
				case "writecsv":
					sparkCmd.writecsv(intext, ds);
					JOptionPane.showMessageDialog(new JFrame(), "csv written to: " + intext);
					consoleTxt.setText(consoleTxt.getText() + "\nwritecsv; " + intext);
					break;
				case "writejson":
					sparkCmd.writejson(intext, ds);
					JOptionPane.showMessageDialog(new JFrame(), "json written to: " + intext);
					consoleTxt.setText(consoleTxt.getText() + "\nwritejson; " + intext);
					break;
				}

			}
		}
	}

	/**
	 * Creates the popup-menu for a table view.
	 * @param  dftabl - the JTable object of the table view.
	 * @return the JPopupMenu for the table view. 
	 */
	private static JPopupMenu createDsPopup(JTable dftabl) {

		JPopupMenu popMenu = new JPopupMenu();
		TabActionListener tabListen = new TabActionListener(dftabl);

		JMenuItem itm2 = new JMenuItem("filter");
		itm2.addActionListener(tabListen);
		popMenu.add(itm2);

		JMenuItem itm1 = new JMenuItem("limit");
		itm1.addActionListener(tabListen);
		popMenu.add(itm1);

		JMenuItem itm3 = new JMenuItem("distinct");
		itm3.addActionListener(tabListen);
		popMenu.add(itm3);
		
		JMenuItem itm9 = new JMenuItem("-------------");
		popMenu.add(itm9);

		JMenuItem itm7 = new JMenuItem("copy_headers");  
		itm7.addActionListener(tabListen);
		popMenu.add(itm7);

		JMenuItem itm4 = new JMenuItem("writepq_and_load");
		itm4.addActionListener(tabListen);
		popMenu.add(itm4);
		
		JMenuItem itm5 = new JMenuItem("writecsv");
		itm5.addActionListener(tabListen);
		popMenu.add(itm5);
		
		JMenuItem itm6 = new JMenuItem("writejson");
		itm6.addActionListener(tabListen);
		popMenu.add(itm6);
		
		return popMenu;
	}

	/**
	 * ActionListener for the menu-entries 'exec command' and 'exec marked'. <br>
	 * Takes the text and calls 'sparkCmd.execTxt'.
	 */
	private static ActionListener execCmd = new ActionListener() {
		@Override
		public void actionPerformed(ActionEvent e) {
			try {
				String name = e.getActionCommand();
				String txt = name.equalsIgnoreCase("exec marked") ? consoleTxt.getSelectedText() : consoleTxt.getText();
				String cmdStr = txt;
				String nam = sparkCmd.execTxt(cmdStr, consoleTxt);

				if (nam == null) {
					JOptionPane.showMessageDialog(null, "command executed");
				} else {
					Dataset<Row> dataset = sparkCmd.getSpark().table(nam);
					performopen(dataset, nam, nam, limit0);
				}
			} catch (Exception ee) {
				JOptionPane.showMessageDialog(null, ee.getMessage(), "execute error",
						JOptionPane.ERROR_MESSAGE);
				return;
			}

		}
	};

	/**
	 * ActionListener for the menu-entry that is a table name of the catalog.<br>
	 * Takes the table-name and calls 'performopen'.
	 */
	private static ActionListener openTable = new ActionListener() {
		@Override
		public void actionPerformed(ActionEvent e) {
			String name = e.getActionCommand();// getSource()
			Dataset<Row> dataset = sparkCmd.getSpark().table(name);
			performopen(dataset, name, name, limit0, false);
			
		}
	};

	/**
	 * The String that holds the readme text.
	 */
	private static String readmeStr = null;
	/**
	 * ActionListener for the menu-entry 'readme'.<br>
	 * Loads README.md and opens a Text View..
	 */
	private static ActionListener showReadme = new ActionListener() {
		@Override
		public void actionPerformed(ActionEvent e) {
			if (readmeStr == null) {
			  try {
				InputStream is = getClass().getResourceAsStream("/de/equbotic/sparkviewer/README.md");
			    readmeStr = IOUtils.toString(is);
			    is.close();
			  } catch (Exception ee) {
				  readmeStr = "readme could not be loaded        \n  ";
			  }
			}
			
			JTextArea readme = new JTextArea();
			readme.setEditable(false);
			readme.setText(readmeStr);

			JFrame frame = new JFrame("Help - readme");
			frame.getContentPane().add(new JScrollPane(readme));
			frame.setPreferredSize(new Dimension(800, 500));
			frame.pack();
			frame.setVisible(true);
		}
	};
	
	/**
	 * ActionListener for the menu-entry 'about'.<br>
	 * Shows about information in a dalog box.
	 */
	private static ActionListener showAbout = new ActionListener() {
		@Override
		public void actionPerformed(ActionEvent e) {
			String aboutstr = "SparkViewer version " + version
					        + "\nCopyright (c) 2018"
					        + "\nDariusSchneider luca@equbotic.com"
					        + "\nhttps://github.com/DariusSchneider/SparkViewer"
					        + "\nhttp://www.equbotic.com/SparkViewer (in work)";
			JOptionPane.showMessageDialog(null, aboutstr, "About", JOptionPane.INFORMATION_MESSAGE);
		}
	};

	/**
	 * ActionListener for the menu-entry 'refresh'.<br>
	 * Clears the menu and creates it again.
	 */
	private static ActionListener refreshMenu = new ActionListener() {
		@Override
		public void actionPerformed(ActionEvent e) {
			theTablesMenu.removeAll();
			addTablesMenuItems();
		}
	};

    /**
	 * The Prefix that filters the list of tables that is shown in the tables menu.<br>
	 * If empty all is shown.
     */
	private static String tablePrefix = ""; 
	/**
	 * ActionListener for the menu-entry 'Prefix'.<br>
	 * Sets the prefix and refreshs the menu.
	 */
	private static ActionListener setTablePrefix = new ActionListener() {
		@Override
		public void actionPerformed(ActionEvent e) {
			String instr = JOptionPane.showInputDialog("set table prefix", tablePrefix);
			instr = instr.trim();
			if (instr != null) {
				tablePrefix = instr.toLowerCase();
				theTablesMenu.removeAll();
				addTablesMenuItems();
		    }
		}
	};

    /**
     * Holds the last drectory where a table s opened.<br>
     * A Open-File Dialog starts in thd directory. 
     */
	private static File lastDir = null;
	/**
	 * Setter for lastDir variable.
	 */
	public  static void setLastDir(String dirpath) {
		lastDir = new File(dirpath);
	}
	/**
	 * ActionListener for the menu-entry 'open file'.<br>
	 * Starts a File-Open dialog and calls 'sparkCmd.open'.<br>
	 * When open was succesfull a new table-view is opened with 'performopen'.
	 */
	private static ActionListener openFile = new ActionListener() {
		@Override
		public void actionPerformed(ActionEvent e) {
			JFileChooser jFileChooser = new JFileChooser(lastDir);
			jFileChooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);//DIRECTORIES_ONLY);
			int result = jFileChooser.showOpenDialog(new JFrame());
			if (result == JFileChooser.APPROVE_OPTION) {
				File lastFile = jFileChooser.getSelectedFile();
				String selstr = lastFile.getAbsolutePath().replace('\\','/');
				String tab = null;
				try {
				   tab = sparkCmd.open(selstr);
				   setLastDir(selstr.substring(0, selstr.lastIndexOf('/')));
				}
				catch (Exception ee) { //ee.getMessage() for fullerrortext
					System.out.println("ERROR: " + ee.getMessage());
					JOptionPane.showMessageDialog(null, "File could not be loaded : " + selstr, 
							"execute error", JOptionPane.ERROR_MESSAGE);
					return;					
				}
				Dataset<Row> dataset = sparkCmd.getSpark().table(tab);
				performopen(dataset, tab, tab, limit0);
			}
		}
	};

	/**
	 * Adds the menu items for the table actions to the menu 'theMenu'.
	 */
	private static void addTablesMenuItems() { // add to theMenu
		List<Table> tabs = sparkCmd.getSpark().catalog().listTables().collectAsList();

		JMenuItem itm6 = new JMenuItem("Refresh Tablelist");
		itm6.addActionListener(refreshMenu);
		theTablesMenu.add(itm6);
		JMenuItem itm2 = new JMenuItem("Prefix: " + tablePrefix); 
		itm2.addActionListener(setTablePrefix);
		theTablesMenu.add(itm2);
		JMenuItem itm5 = new JMenuItem("-------------");
		theTablesMenu.add(itm5);

		for (Table tab : tabs) {
			if (tablePrefix.isEmpty() || tab.name().toLowerCase().startsWith(tablePrefix)) {
				JMenuItem itm = new JMenuItem(tab.name());
				itm.addActionListener(openTable);
				theTablesMenu.add(itm);
			}
		}
	}

	/**
	 * Creates the main menu bar.
	 * @return the JMenuBar of the application
	 */
	private static JMenuBar createMenuBar() {
		// ----------------------------------------------------------------
		JMenu execmenu = new JMenu("Exec/Open");
		JMenuItem itm1 = new JMenuItem("Exec Command");
		itm1.addActionListener(execCmd);
		execmenu.add(itm1);
		JMenuItem itm2 = new JMenuItem("Exec Marked");
		itm2.addActionListener(execCmd);
		execmenu.add(itm2);
		JMenuItem itm5 = new JMenuItem("-------------");
		execmenu.add(itm5);
		JMenuItem itmc1 = new JMenuItem("Open File (infer type)"); 
		itmc1.addActionListener(openFile);
		execmenu.add(itmc1);
		// ----------------------------------------------------------------
		JMenu helpmenu = new JMenu("Help");
		JMenuItem itmh1 = new JMenuItem("ReadMe"); 
		itmh1.addActionListener(showReadme);
		helpmenu.add(itmh1);
		JMenuItem itmh2 = new JMenuItem("About");  
		itmh2.addActionListener(showAbout);
		helpmenu.add(itmh2);

		// ----------------------------------------------------------------
		theTablesMenu = new JMenu("Tables");
		addTablesMenuItems();

		// ----------------------------------------------------------------
		JMenuBar bar = new JMenuBar();
		bar.add(execmenu);
		bar.add(theTablesMenu);
		bar.add(helpmenu);

		return bar;
	}

	/**
	 * Opens a dataframe in the tables view by calling 'performopenArr'.
	 * @param datasetin - the dataset to be shown.
	 * @param tabname - the tabname of the dataset in the catalog.
	 * @param cmdstr - the command, when dataset is build with a command. 
	 * @param limi - the limit size for rows to show.
	 * @param addmenu - boolean, if 'tabname' should be added to the tables menu.
	 */
	public static void performopen(Dataset<Row> datasetin, String tabname, String cmdstr, int limi, boolean addmenu) {
		String nameOrCmd = (cmdstr != null) ? cmdstr : tabname;

		Dataset<Row> dataset = datasetin.limit(limi);
		long dfanz = datasetin.count();
		String stranz = (dfanz > limi) ? limi + "/" + dfanz : "" + dfanz;

		List<Row> listRow = dataset.collectAsList();

		StructType schema = dataset.schema();

		performopenArr((Row[]) listRow.toArray(), schema, nameOrCmd + " (" + stranz + ")", tabname, datasetin);
		if (addmenu && tabname != null && !tabname.isEmpty() ) {
			JMenuItem itm = new JMenuItem(tabname);
			itm.addActionListener(openTable);
			theTablesMenu.add(itm);
		}
	}
	/**
	 * Calls 'performopen' with paramter 'addmenu' = true. 
	 * @see  performopen
	 */
	public static void performopen(Dataset<Row> datasetin, String tabname, String cmdstr, int limi) {
		performopen(datasetin, tabname, cmdstr, limi, true);
	}

	/**
	 * Constructs a DefaultMutableTreeNode for a Row.<br>
	 * When a 'txt' is given, then the Text for the node is 'txt', otherwise Row.toString is used.<br>
	 * Substructures of Row are filled and added recursive.  
	 * @param rr - the Row for the node.
	 * @param txt - optional, filled as node text. 
	 * @return the DefaultMutableTreeNode for Row.
	 */
	public static DefaultMutableTreeNode getRowTree(Row rr, String txt) {
		DefaultMutableTreeNode rnode = new DefaultMutableTreeNode((txt != null) ? txt : rr.toString());
		String[] fldnams = rr.schema().fieldNames();
		for (int ii = 0; ii < fldnams.length; ii++) {
			Object oo = rr.get(ii);
			String nodstr = fldnams[ii] + " : " + ((oo == null) ? "null" : oo.toString());

			DefaultMutableTreeNode inode = null;
			if (oo != null && oo instanceof WrappedArray<?>) {
				@SuppressWarnings("unchecked")
				WrappedArray<Row> wa = (WrappedArray<Row>) oo;
				Row[] ww = (Row[]) wa.array();
				inode = new DefaultMutableTreeNode(nodstr);
				for (int i2 = 0; i2 < ww.length; i2++) {
					inode.add(getRowTree(ww[i2], ("element_" + (i2 + 1))));
				}
			} else if (oo != null && oo instanceof GenericRowWithSchema) {
				Row ro = ((GenericRowWithSchema) oo);
				inode = getRowTree(ro, nodstr);
			} else
				inode = new DefaultMutableTreeNode(nodstr);

			rnode.add(inode);
		}
		return rnode;
	}

	/**
	 * Builds the JTree for the given Row array, by calling 'getRowTree' for every Row. 
	 * @param rrr - the Row array. 
	 * @return the JTree for the Row array.
	 */
	public static JTree getTree(Row[] rrr) {
		DefaultMutableTreeNode root = new DefaultMutableTreeNode("ROOT");
		DefaultTreeModel model = new DefaultTreeModel(root);
		JTree tree = new JTree(model);
		for (Row rr : rrr) {
			root.add(getRowTree(rr, null));
		}
		return tree;
	}

	/**
	 * Open a row array in a tables view with three tabs:<br>
	 * - the tables view (JTable) <br>
	 * - a tree View (JTree) <br>
	 * - the schema (JTextArea) <br>
	 * 
	 * @param rrr - the Row array.
	 * @param schema - the schema for a Row.
	 * @param cmdstr - the command, when dataset is build with a command. 
	 * @param tabname - the tabname of the dataset in the catalog.
	 * @param ds - optional, the Dataset behind the Rows.  
	 */
	public static void performopenArr(Row[] rrr, StructType schema, String cmdstr, String tabname, Dataset<Row> ds) {
		String nameOrCmd = (cmdstr != null) ? cmdstr : tabname;
		consoleTxt.setText(consoleTxt.getText() + "\n" + nameOrCmd);

		StringBuffer rowStrBuf = new StringBuffer();
		for (Row row : rrr) {
			rowStrBuf.append(row.toString() + "\n");
		}

		DfTableModel dftabmod = new DfTableModel(rrr, schema, nameOrCmd, ds, tabname);

		// =====================================================
		/**
		 * Define a local JTable for this 'DfTableModel'.<br>
		 * Allow 'ToolTipText' to show the complete value of a cell.  
		 */
		JTable dftable = new JTable(dftabmod) {
			private static final long serialVersionUID = 1L;

			public String getToolTipText(MouseEvent e) {
				String tip = null;
				java.awt.Point p = e.getPoint();
				int rowIndex = rowAtPoint(p);
				int colIndex = columnAtPoint(p);
				try {
					tip = getValueAt(rowIndex, colIndex).toString();
					if (tip.length() > 200)
						tip = tip.substring(0, 199) + "...";
				} catch (RuntimeException e1) {
					// catch null pointer exception if mouse is over an empty line
				}
				return tip;
			}
		};
		dftable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
		dftable.setAutoCreateRowSorter(true);
		dftable.setFillsViewportHeight(true);
		dftable.addMouseListener(openClick);
		if (ds != null)
			dftable.setComponentPopupMenu(createDsPopup(dftable));

		JTree treeDs = getTree(rrr);
		treeDs.setEditable(false);

		JTextArea txtschema = new JTextArea();
		txtschema.setEditable(false);
		txtschema.setText(schema.treeString());

		// -----------------------
		JTabbedPane tabs = new JTabbedPane(JTabbedPane.TOP);
		tabs.addTab("table", new JScrollPane(dftable));
		tabs.addTab("tree", new JScrollPane(treeDs));
		tabs.addTab("schema", new JScrollPane(txtschema));
		tabs.setPreferredSize(new Dimension(800, 500));

		JFrame frame = new JFrame(nameOrCmd);
		frame.setLocationByPlatform(true);
		frame.getContentPane().add(tabs);
		frame.pack();
		frame.setVisible(true);
	}

	/**
	 * Creates the gui by showing the console view with the main menu. 
	 */
	private static void createAndShowGUI() {
		JFrame frame = new JFrame("SparkViewer Console (" + version + ")"); // Console (do not close)
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);// .DO_NOTHING_ON_CLOSE); //EXIT_ON_CLOSE);

		consoleTxt = new JTextArea();
		consoleTxt.setText("\n\n\n"
				+ "===============================================================================================================\n"
				+ "opentable via menu 'Tables' or menu 'Open>File'\n\n" + "commands:\n" 
				+ "open;       \tFileName; \tTableName (infer type)\n"   
				+ "execsql;    \tSqlStr;   \tTableName\n" 
				+ "writepq;    \tFileName; \tTableName (makes reread)\n"
				+ "writecsv;   \tFileName; \tTableName\n"
				+ "writejson;  \tFileName; \tTableName\n\n"
				+ "execute via menu 'Exec' the marked text or the text at top, above first line starting with '=' \n"
				+ "multiple commands can be separated with a line starting with '-'\n"
				+ "Comments can be placed at then end of a line starting with '-'\n"
				+ "===============================================================================================================\n");

		frame.getContentPane().add(new JScrollPane(consoleTxt));
		frame.setJMenuBar(createMenuBar());
		frame.setPreferredSize(new Dimension(900, 700));
		frame.pack();
		frame.setVisible(true);
	}

	/**
	 * The actual version number.
	 */
	private static String version = "0.7.0";
	/**
	 * The JTextArea for the console. 
	 */
	private static JTextArea consoleTxt = null;
	/**
	 * The JMenu for the tables menu.
	 * @see addTablesMenuItems
	 */
	private static JMenu theTablesMenu = null;
	/**
	 * The SparkCmds object for this session.
	 * @see SparkCmds
	 */
	private static SparkCmds sparkCmd = null;
	
	/**
	 * Starts the swing gui for the given SparkCmds object.
	 * @param sprkCmd
	 */
	public static void start(SparkCmds sprkCmd) { 
		sparkCmd = sprkCmd;

		javax.swing.SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				createAndShowGUI();
			}
		});
	}
}