package ma.enset;

import java.util.ArrayList;
import java.util.List;

public class Centroid extends  Point {
    private double centre;
    List<Point>points=new ArrayList<>();

    public Centroid(double centre,double x,double y) {
        super(x,y);
        this.centre = centre;
    }

    public Centroid() {
    }

    @Override
    public String toString() {
        return "Centroid{" +
                "[centrex=" + getX() +
                "centreY=" + getY() +
                "], points=" + points +
                '}';
    }

    public double getCentre() {
        return centre;
    }

    public void setCentre(double centre) {
        this.centre = centre;
    }

    public List<Point> getPoints() {
        return points;
    }

    public void setPoints(List<Point> points) {
        this.points = points;
    }
}
