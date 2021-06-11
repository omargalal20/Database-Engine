
import java.io.Serializable;
import java.util.Vector;

public class GridIndex implements Serializable {
	private String name;
	private Vector<String> Indexes;
	private Vector<String> bucketNames;

	public Vector<String> getIndexes() {
		return Indexes;
	}

	public void setIndexes(Vector<String> indexes) {
		Indexes = indexes;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	private Vector GridIndex;

	public Vector getGridIndex() {
		return GridIndex;
	}

	public void setGridIndex(Vector gridIndex) {
		GridIndex = gridIndex;
	}

	public GridIndex() {
		name = "";
		GridIndex = new Vector();
		Indexes = new Vector<String>();
		bucketNames = new Vector<String>();
	}

	public Vector<String> getBucketNames() {
		return bucketNames;
	}

	public void setBucketNames(Vector<String> bucketNames) {
		this.bucketNames = bucketNames;
	}

}
