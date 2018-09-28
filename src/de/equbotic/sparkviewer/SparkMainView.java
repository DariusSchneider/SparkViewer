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

import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.util.List;
import javax.swing.*;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import scala.collection.mutable.WrappedArray;

public class SparkMainView {
	public static int limit0 = 200;
	/**
	 * double click in JTable to open new TableView for substructures
	 */
	private static MouseAdapter openClick = new MouseAdapter() {
		public void mouseClicked(MouseEvent e) {
			if (e.getClickCount() == 2) {
				JTable tabl = (JTable) e.getSource();
				int rr = tabl.getSelectedRow();
				int cc = tabl.getSelectedColumn();

				Object cel = tabl.getModel().getValueAt(rr, cc);

				if (cel != null && (cel instanceof WrappedArray<?> || cel instanceof GenericRowWithSchema)) {

					DfTableModel mymod = (DfTableModel) tabl.getModel();
					String colnam = mymod.getColumnName(cc);
					String titl = mymod.getTitleBas() + " - " + colnam + " of row " + (rr + 1);

					if (cel instanceof WrappedArray<?>) {
						WrappedArray<Row> wa = (WrappedArray<Row>) cel;
						Row[] oo = (Row[]) wa.array();
						StructType schema = oo[0].schema();

						performopenArr(oo, schema, titl, null);
					} else if (cel instanceof GenericRowWithSchema) {
						Row ro = ((GenericRowWithSchema) cel);
						Row[] oo = new Row[] { ro };
						StructType schema = ro.schema();

						performopenArr(oo, schema, titl, null);
					}
				}
			}
		}
	};

	/**
	 * Actions for vor JTable PopUpMenu : Filter, Limit, distinct
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

			// String intext = JOptionPane.showInputDialog(e.getSource(), colnam);
			String intext = JOptionPane.showInputDialog(name, colnam);
			if (intext != null) {

				Dataset<Row> ds = dfTabMod.getDataFrame();

				switch (name) {
				case "filter":
					Dataset<Row> dsfil = ds.filter(intext);
					performopen(dsfil, titlBas + " > filter:" + intext, limit0);
					break;

				case "limit":
					performopen(ds, titlBas, Integer.parseInt(intext));
					break;

				case "distinct":
					Dataset<Row> dsdis = ds.groupBy(intext).count().sort(intext);
					performopen(dsdis, titlBas + " > distinct:" + intext, limit0);
					break;

				}

			}
		}
	}

	/**
	 * popupmenu vor JTable
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

		return popMenu;
	}

	/**
	 * Actions vor JMenu
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
					JOptionPane.showMessageDialog(new JFrame(), "command executed");
				} else {
					Dataset<Row> dataset = sparkCmd.getSpark().table(nam);
					performopen(dataset, nam, limit0);
					JMenuItem itm = new JMenuItem(nam);
					itm.addActionListener(openTable);
					theMenu.add(itm);
				}
			} catch (Exception ee) {
				JOptionPane.showMessageDialog(new JFrame(), ee.getMessage(), "execute error",
						JOptionPane.ERROR_MESSAGE);
				return;
			}

		}
	};

	private static ActionListener openTable = new ActionListener() {
		@Override
		public void actionPerformed(ActionEvent e) {
			String name = e.getActionCommand();// getSource()
			Dataset<Row> dataset = sparkCmd.getSpark().table(name);
			performopen(dataset, name, limit0);
		}
	};

	private static ActionListener refreshMenu = new ActionListener() {
		@Override
		public void actionPerformed(ActionEvent e) {
			theMenu.removeAll();
			addMenuItems();
		}
	};
	private static File lastDir = null;
	public  static void setLastDir(String dirpath) {
		lastDir = new File(dirpath);
	}
	private static ActionListener openFile = new ActionListener() {
		@Override
		public void actionPerformed(ActionEvent e) {
			JFileChooser jFileChooser = new JFileChooser(lastDir);
			jFileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
			int result = jFileChooser.showOpenDialog(new JFrame());
			if (result == JFileChooser.APPROVE_OPTION) {
				File lastFile = jFileChooser.getSelectedFile();
				String selstr = lastFile.getAbsolutePath();
				setLastDir(selstr.substring(0, selstr.lastIndexOf('/')));
				String tab = null;
				try {
				   tab = sparkCmd.execCmd("openfile;" + selstr); // TODO exec openfile as function
				}
				catch (Exception ee) {
					JOptionPane.showMessageDialog(new JFrame(), ee.getMessage(), "execute error",
							JOptionPane.ERROR_MESSAGE);
					return;					
				}
				Dataset<Row> dataset = sparkCmd.getSpark().table(tab);
				performopen(dataset, tab, limit0);
			}

		}
	};

	/**
	 * add menu Items for main menu
	 */
	private static void addMenuItems() { // add to theMenu
		List<Table> tabs = sparkCmd.getSpark().catalog().listTables().collectAsList();

		// TODO write JSON
		// tname.repartition(1).write.json("C:/tmp/hive/_tmp/json_someID_timestamp")
		// or pretty Json val rpPretty = pretty (rpJval) //is a pretty JSON-String
		// toFile(filename + ".json", rpPretty) //siehe separate scripte

		// TODO separates menu fuer tabs mit >define prefix<
		JMenuItem itm6 = new JMenuItem("refresh tablelist");
		itm6.addActionListener(refreshMenu);
		theMenu.add(itm6);
		JMenuItem itm2 = new JMenuItem("prefix (todo)");
		// itm2.addActionListener(openFile);
		theMenu.add(itm2);
		JMenuItem itm5 = new JMenuItem("-------------");
		theMenu.add(itm5);

		for (Table tab : tabs) {
			JMenuItem itm = new JMenuItem(tab.name());
			itm.addActionListener(openTable);
			theMenu.add(itm);
		}
	}

	/**
	 * create main menu bar
	 */
	private static JMenuBar createMenuBar() {
		// ----------------------------------------------------------------
		JMenu execmenu = new JMenu("execute");
		JMenuItem itm1 = new JMenuItem("exec command");
		itm1.addActionListener(execCmd);
		execmenu.add(itm1);
		JMenuItem itm2 = new JMenuItem("exec marked");
		itm2.addActionListener(execCmd);
		execmenu.add(itm2);
		JMenuItem itm5 = new JMenuItem("-------------");
		execmenu.add(itm5);
		// TODO one menu for cmnds. Params taken from DataFrameCmds. With Generic Dialog
		// to open
		JMenuItem itmc1 = new JMenuItem("open file");
		itmc1.addActionListener(openFile);
		execmenu.add(itmc1);
		// ----------------------------------------------------------------
		JMenu helpmenu = new JMenu("help");
		JMenuItem itmh1 = new JMenuItem("readme (todo)"); // show readme.txt
		// itmh1.addActionListener(openFile);
		helpmenu.add(itmh1);
		JMenuItem itmh2 = new JMenuItem("about (todo)");
		// itmh2.addActionListener(openFile);
		helpmenu.add(itmh2);

		// ----------------------------------------------------------------
		theMenu = new JMenu("tables");
		addMenuItems();

		// ----------------------------------------------------------------
		JMenuBar bar = new JMenuBar();
		bar.add(execmenu);
		bar.add(theMenu);
		bar.add(helpmenu);

		return bar;
	}

	/**
	 * open a dataframe in a dataframeview - calls performopenArr
	 */
	public static void performopen(Dataset<Row> datasetin, String nameOrCmd, int limi) {

		Dataset<Row> dataset = datasetin.limit(limi);
		long dfanz = datasetin.count();
		String stranz = (dfanz > limi) ? limi + "/" + dfanz : "" + dfanz;

		List<Row> listRow = dataset.collectAsList();

		StructType schema = dataset.schema();

		performopenArr((Row[]) listRow.toArray(), schema, nameOrCmd + " (" + stranz + ")", datasetin);
	}

	public static DefaultMutableTreeNode getRowTree(Row rr, String txt) {
		DefaultMutableTreeNode rnode = new DefaultMutableTreeNode((txt != null) ? txt : rr.toString());
		String[] fldnams = rr.schema().fieldNames();
		for (int ii = 0; ii < fldnams.length; ii++) {
			Object oo = rr.get(ii);
			String nodstr = fldnams[ii] + " : " + ((oo == null) ? "null" : oo.toString());

			DefaultMutableTreeNode inode = null;
			if (oo != null && oo instanceof WrappedArray<?>) {
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
	 * open a row array in a dataframeview
	 */
	public static void performopenArr(Row[] rrr, StructType schema, String nameOrCmd, Dataset<Row> ds) {
		consoleTxt.setText(consoleTxt.getText() + "\n" + nameOrCmd);

		StringBuffer rowStrBuf = new StringBuffer();
		for (Row row : rrr) {
			rowStrBuf.append(row.toString() + "\n");
		}

		DfTableModel dftabmod = new DfTableModel(rrr, schema, nameOrCmd, ds);

		// =====================================================
		JTable dftable = new JTable(dftabmod) {
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
		dftable.setFillsViewportHeight(true);
		dftable.addMouseListener(openClick);
		if (ds != null)
			dftable.setComponentPopupMenu(createDsPopup(dftable));

		// JTextArea txtrows = new JTextArea();
		// txtrows.setEditable(false);
		// txtrows.setText(rowStrBuf.toString());

		// TODO create a TreeView with JTable subview
		JTree treeDs = getTree(rrr);
		// treeDs.setEditable(false);

		JTextArea txtschema = new JTextArea();
		txtschema.setEditable(false);
		txtschema.setText(schema.treeString());

		// -----------------------
		JTabbedPane tabs = new JTabbedPane(JTabbedPane.TOP);
		tabs.addTab("table", new JScrollPane(dftable));
		// tabs.addTab("rows", new JScrollPane(txtrows));
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
	 * create and show gui
	 */
	private static void createAndShowGUI() {
		// TODO COPYRIGHT + OPENSOURCE
		// TODO comment everything
		// TODO remove warnings

		JFrame frame = new JFrame("SparkViewer Console"); // Console (do not close)
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);// .DO_NOTHING_ON_CLOSE); //EXIT_ON_CLOSE);

		consoleTxt = new JTextArea();
		consoleTxt.setPreferredSize(new Dimension(900, 700));
		// consoleTxt.setComponentPopupMenu(createMainPopup());

		// TODO Text taken from DataFrameCmds
		consoleTxt.setText("\n\n\n"
				+ "===============================================================================================================\n"
				+ "opentable via menu 'tables'\n\n" + "commands:\n" + "openfile;   \tFileName; \ttableName\n"
				+ "execsql;    \tsqlStr;   \ttableName\n" + "writetable; \tFileName; \ttableName (makes reread)\n"
				+ "writecsv;   \tFileName; \ttableName\n\n"
				+ "executed marked or text above '=...'-line via menu 'exec'\n"
				+ "multiple commands can be separated with a '-...' line\n"
				+ "Comments can be placed in a '-...' line \n"
				+ "===============================================================================================================\n");

		frame.getContentPane().add(new JScrollPane(consoleTxt));
		frame.setJMenuBar(createMenuBar());
		frame.pack();
		frame.setVisible(true);
	}

	private static JTextArea consoleTxt = null;
	private static JMenu theMenu = null;
	private static SparkCmds sparkCmd = null;

	/**
	 * start the swing gui
	 */
	public static void start(SparkCmds sprkCmd) { // main(String[] args) {
		sparkCmd = sprkCmd;

		javax.swing.SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				createAndShowGUI();
			}
		});
	}
}