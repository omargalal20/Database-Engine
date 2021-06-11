import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Vector;

public class DBAppTEST {

	public static void modifyFile(String filePath, Vector<String> indexes) {
		File fileToBeModified = new File(filePath);

		String oldContent = "";

		BufferedReader reader = null;

		FileWriter writer = null;

		try {
			reader = new BufferedReader(new FileReader(fileToBeModified));

			// Reading all the lines of input text file into oldContent

			String line = reader.readLine();
			int lineCounter = 0;
			while (line != null) {
				oldContent += line + "," + "\n";
				lineCounter++;
				line = reader.readLine();
			}
			// String newContent = oldContent.replaceAll("false", "true");
			System.out.println(oldContent);
			System.out.println("oldContent");
			String[] arr = oldContent.split(",");
			System.out.println(arr.length);
			System.out.println(lineCounter + " linecounter");
			System.out.println();
			System.out.println("Start");

			int j = 0;
			for (int i = 0; i < arr.length; i++) {
				if (j == 7) {
					j = 0;
					if (lineCounter == 0) {
						break;
					} else
						lineCounter--;
				} else if (j == 4) {
					for (String s : indexes) {
						if (arr[(i - j) + 1].equals(s))
							arr[i] = "true";
					}
				}
				System.out.println(arr[i] + " " + j + " " + i);
				j++;
			}
			System.out.println("Modified");
			writer = new FileWriter("src/main/resources/metadata.csv");
			BufferedWriter bw = new BufferedWriter(writer);
			String l1 = "";
			j = 0;
			for (String s : arr) {
				if (j == 6) {
					j = 0;
					l1 += s;
					System.out.println(l1);
					bw.write(l1);
					//bw.newLine();
					l1 = "";
				} else {
					l1 += (s + ",");
					j++;
				}
			}
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		Vector<String> indexes = new Vector<String>();
		indexes.add("gpa");
		indexes.add("id");
		modifyFile("src/main/resources/metadata.csv", indexes);
	}

}
