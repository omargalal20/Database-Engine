import java.io.Serializable;
import java.util.Vector;

public class Row implements Serializable {
	Vector<Object> values;

	public Vector<Object> getValues() {
		return values;
	}

	public void setValues(Vector<Object> values) {
		this.values = values;
	}

	public Row() {
		values = new Vector<Object>();
	}

	public void addvalue(Object value) {
		// TODO Auto-generated method stub
		values.add(value);
	}
}
