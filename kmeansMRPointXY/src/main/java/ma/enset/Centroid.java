package ma.enset;

import java.util.ArrayList;
import java.util.List;

public class Centroid extends  Point {
    List<Point>points=new ArrayList<>();
    public Centroid(double x,double y) {
        super(x,y);

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


    public List<Point> getPoints() {
        return points;
    }

    public void setPoints(List<Point> points) {
        this.points = points;
    }
}
