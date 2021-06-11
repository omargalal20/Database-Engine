import java.io.Serializable;
import java.util.Vector;

public class Bucket implements Serializable {

	Vector PKs;
	Bucket OverflowBucket;
	Vector<Integer> overFlowNames;

	public Vector getPKs() {
		return PKs;
	}

	public void setPKs(Vector pKs) {
		PKs = pKs;
	}

	Vector<Row> BucketRow;
	String BucketName;

	public Vector<Row> getBucketRow() {
		return BucketRow;
	}

	public void setBucketRow(Vector<Row> bucketRow) {
		BucketRow = bucketRow;
	}

	public String getBucketName() {
		return BucketName;
	}

	public void setBucketName(String bucketName) {
		BucketName = bucketName;
	}

	public Bucket() {
		BucketRow = null;
		BucketName = "";
		PKs = null;
		OverflowBucket = null;
		overFlowNames = null;
	}
}
