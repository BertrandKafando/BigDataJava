package ma.enset;

import java.util.ArrayList;
import java.util.List;

public class Centroid {
    private double center;
    private ArrayList<Pixel> pixels;

    public Centroid(double center, ArrayList<Pixel> pixels) {
        this.center = center;
        this.pixels = pixels;
    }

    public Centroid(double center) {
        this.center = center;
    }

    public Centroid() {
    }

    public double getCenter() {
        return center;
    }

    public void setCenter(double center) {
        this.center = center;
    }

    public ArrayList<Pixel> getPixels() {
        return pixels;
    }

    public void setPixels(ArrayList<Pixel> pixels) {
        this.pixels = pixels;
    }
}