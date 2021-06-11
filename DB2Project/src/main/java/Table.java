import java.io.Serializable;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

public class Table implements Serializable {

	private static final long serialVersionUID = 1L;
	String Name;
	String PrimaryKey;
	boolean clusteringKey;
	Vector<String> columnsName;
	Vector<String> columnsType;
	Vector<String> overflowNames;
	Hashtable<String, String> colNameValue;
	Hashtable<String, String> max;
	Hashtable<String, String> min;
	boolean indexed;
	transient int N;
	transient int P;
	transient int C;
	int bucketSize;
	Vector<String> PageNames;
	Vector<String> GridNames;
	Vector<GridIndex> GridIndeces;
	Vector<Boolean> keysWithIndex;
	Vector PKs;
	Vector PksOverflow;
	int PKindex;
	String PKType;

	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax, int maxTuples,
			int bucketListSize, int pagesNumber) {
		GridIndeces = new Vector<GridIndex>();
		GridNames = new Vector<String>();
		bucketSize = bucketListSize;
		PksOverflow = new Vector();
		colNameValue = new Hashtable<String, String>();
		Name = strTableName;
		N = maxTuples;
		clusteringKey = false;
		PrimaryKey = strClusteringKeyColumn;
		min = htblColNameMin;
		max = htblColNameMax;
		P = pagesNumber;
		PageNames = new Vector<String>();
		overflowNames = new Vector<String>();
		columnsName = new Vector<String>();
		columnsType = new Vector<String>();
		C = 0;
		PKs = new Vector();
		PKindex = 0;
		PKType = "";
		Set<String> keys = htblColNameType.keySet();
		Collection<String> values = htblColNameType.values();
		
		keysWithIndex = new Vector<Boolean>();
		
		for (String key : keys) {
			columnsName.add(key);
			keysWithIndex.add(false);
		}
		for (String value : values) {
			columnsType.add(value);
		}
	}

	public static void Insertion(String strTableName, Hashtable<String, Object> htblColNameValue) {
	}

	public String getName() {
		return Name;
	}

	public void setName(String name) {
		Name = name;
	}

	public String getPrimaryKey() {
		return PrimaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		PrimaryKey = primaryKey;
	}

	public Vector<String> getColumnsName() {
		return columnsName;
	}

	public void setColumnsName(Vector<String> columnsName) {
		this.columnsName = columnsName;
	}

	public Vector<String> getColumnsType() {
		return columnsType;
	}

	public void setColumnsType(Vector<String> columnsType) {
		this.columnsType = columnsType;
	}

	public Hashtable<String, String> getMax() {
		return max;
	}

	public void setMax(Hashtable<String, String> max) {
		this.max = max;
	}

	public Hashtable<String, String> getMin() {
		return min;
	}

	public void setMin(Hashtable<String, String> min) {
		this.min = min;
	}

	public int getN() {
		return N;
	}

	public void setN(int n) {
		N = n;
	}

	public int getP() {
		return P;
	}

	public void setP(int p) {
		P = p;
	}

	public int getC() {
		return C;
	}

	public void setC(int c) {
		C = c;
	}
	
	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	public Hashtable<String, String> getColNameValue() {
		return colNameValue;
	}

	public void setColNameValue(Hashtable<String, String> colNameValue) {
		this.colNameValue = colNameValue;
	}

	public int getPKindex() {
		return PKindex;
	}

	public void setPKindex(int pKindex) {
		PKindex = pKindex;
	}

	public String getPKType() {
		return PKType;
	}

	public void setPKType(String pKType) {
		PKType = pKType;
	}
	
	public Vector<Boolean> getKeysWithIndex() {
		return keysWithIndex;
	}

	public void setKeysWithIndex(Vector<Boolean> keysWithIndex) {
		this.keysWithIndex = keysWithIndex;
	}

	public Vector<String> getPageNames() {
		return PageNames;
	}

	public void setPageNames(Vector<String> pageNames) {
		PageNames = pageNames;
	}
	
	public Vector getPKs() {
		return PKs;
	}

	public void setPKs(Vector pKs) {
		PKs = pKs;
	}

	public Vector getPksOverflow() {
		return PksOverflow;
	}

	public void setPksOverflow(Vector pksOverflow) {
		PksOverflow = pksOverflow;
	}
}