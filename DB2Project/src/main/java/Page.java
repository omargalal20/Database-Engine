
import java.io.Serializable;
import java.util.Vector;

@SuppressWarnings("serial")
public class Page implements Serializable {

	Vector<Row> Rows;
	Object min;
	Object max;

	public Page() {
		Rows = new Vector<Row>();
	}
	
	public void addRows(Vector<Row> row) {
		Rows.addAll(row);
	}

	public Vector<Row> getRow() {
		return Rows;
	}

	public void setRow(Vector<Row> row) {
		this.Rows = row;
	}
}
