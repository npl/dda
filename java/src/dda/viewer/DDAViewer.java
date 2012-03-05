package dda.viewer;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.io.IOException;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.openstreetmap.gui.jmapviewer.JMapViewer;
import org.openstreetmap.gui.jmapviewer.JobDispatcher;
import org.openstreetmap.gui.jmapviewer.MapMarkerDot;
import org.openstreetmap.gui.jmapviewer.OsmFileCacheTileLoader;
import org.openstreetmap.gui.jmapviewer.OsmTileLoader;
import org.openstreetmap.gui.jmapviewer.interfaces.TileLoader;
import org.openstreetmap.gui.jmapviewer.interfaces.TileSource;
import org.openstreetmap.gui.jmapviewer.tilesources.BingAerialTileSource;
import org.openstreetmap.gui.jmapviewer.tilesources.OsmTileSource;

import dda.db.hbase.TileTable;

public class DDAViewer extends JFrame {
	private static final long serialVersionUID = 1L;
	private JMapViewer map;

	public DDAViewer() {
		super("JMapViewer Demo");
		setSize(400, 400);

		map = new JMapViewer();
		setupGui();
		setLocationToKempten();
	}
	
	private void setupGui() {
		// tile source selector
		JComboBox tileSourceSelector = new JComboBox(
				new TileSource[] {
						new OsmTileSource.Mapnik(),
						new OsmTileSource.TilesAtHome(),
						new OsmTileSource.CycleMap(),
						new BingAerialTileSource()
				}
		);
		tileSourceSelector.addItemListener(new ItemListener() {
			public void itemStateChanged(ItemEvent e) {
				map.setTileSource((TileSource) e.getItem());
			}
		});

		// tile loader selector
		JComboBox tileLoaderSelector;
		try {
			tileLoaderSelector = new JComboBox(
					new TileLoader[] {
							new OsmFileCacheTileLoader(map),
							new OsmTileLoader(map)
					}
			);
		} catch (IOException e) {
			tileLoaderSelector = new JComboBox(new TileLoader[] { new OsmTileLoader(map) });
		}
		tileLoaderSelector.addItemListener(new ItemListener() {
			public void itemStateChanged(ItemEvent e) {
				map.setTileLoader((TileLoader) e.getItem());
			}
		});
		
		// markers visible checkbox
		final JCheckBox showMapMarker = new JCheckBox("Map markers visible");
		showMapMarker.setSelected(map.getMapMarkersVisible());
		showMapMarker.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				map.setMapMarkerVisible(showMapMarker.isSelected());
			}
		});
		
		// grid visible checkbox
		final JCheckBox showTileGrid = new JCheckBox("Tile grid visible");
		showTileGrid.setSelected(map.isTileGridVisible());
		showTileGrid.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {
				map.setTileGridVisible(showTileGrid.isSelected());
			}
		});
		
		// show zoom controls checkbox
		final JCheckBox showZoomControls = new JCheckBox("Show zoom controls");
		showZoomControls.setSelected(map.getZoomContolsVisible());
		showZoomControls.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {
				map.setZoomContolsVisible(showZoomControls.isSelected());
			}
		});
		
		final JCheckBox showDensityMap = new JCheckBox("Show density map");
		showDensityMap.setSelected(map.isDensityMapVisible());
		showDensityMap.addActionListener(new ActionListener() {
			
			public void actionPerformed(ActionEvent e) {
				map.setDensityMapVisible(showDensityMap.isSelected());
			}
		});
		
		// set display to fit markers - button
		JButton setDisplayToFitMarkersButton = new JButton("setDisplayToFitMapMarkers");
		setDisplayToFitMarkersButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				map.setDisplayToFitMapMarkers();
			}
		});
		
		// help label
		JLabel helpLabel = new JLabel("Use right mouse button to move,\nleft double click or mouse wheel to zoom.");
		
		setLayout(new BorderLayout());
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setExtendedState(JFrame.MAXIMIZED_BOTH);

		JPanel topPanel = new JPanel();
		topPanel.add(tileSourceSelector);
		topPanel.add(tileLoaderSelector);
		topPanel.add(showMapMarker);
		topPanel.add(showTileGrid);
		topPanel.add(showDensityMap);
		topPanel.add(setDisplayToFitMarkersButton);

		JPanel bottomPanel = new JPanel();
		bottomPanel.add(helpLabel);

		add(topPanel, BorderLayout.NORTH);
		add(map, BorderLayout.CENTER);
		add(bottomPanel, BorderLayout.SOUTH);

		map.setTileSource((TileSource) tileSourceSelector.getSelectedItem());
		map.setTileLoader((TileLoader) tileLoaderSelector.getSelectedItem());
	}

	private void setLocationToKempten() {
		double kemptenLat = 47.7230;
		double kemptenLon = 10.3029;
		map.addMapMarker(new MapMarkerDot(kemptenLat, kemptenLon));
		map.setDisplayPositionByLatLon(kemptenLat, kemptenLon, 17);
	}

	public static void main(String[] args) {
		if (args.length >= 2) {
			System.out.println(String.format("using HBase-Table : '%s'", args[0]));
			
			JobDispatcher.WORKER_THREAD_MAX_COUNT=8;
			
			TileTable.setZookeeperHost(args[0]);
			TileTable.setTableName(args[1]);
			if (args.length == 3)
				DensityTilePainter.setGranuldarity(Integer.parseInt(args[2]));

			new DDAViewer().setVisible(true);
		} else {
			System.err.println("Usage: program <zookeer-host> <hbase-table> [<pixel-granularity-level>] ");
		}
	}
}
