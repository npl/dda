package dda.viewer;

import java.awt.Color;
import java.awt.Graphics;

public class BigGrid {
	public static void drawGrid(Graphics g, int posx, int posy, int tilesize, int zoom) {
		int refinementFactor = Refinement.getRefinementFactor(zoom);
		int steps = 1<<refinementFactor;
		int numPixels = 256/steps;
	
		g.setColor(Color.lightGray);
		for (int xOffset=0; xOffset<steps; xOffset++)
			for (int yOffset=0; yOffset<steps; yOffset++)
				g.drawRect(posx + numPixels*xOffset, posy + numPixels*yOffset, numPixels, numPixels);
			
		g.setColor(Color.BLACK);
		g.drawRect(posx, posy, tilesize, tilesize);
	}
}
