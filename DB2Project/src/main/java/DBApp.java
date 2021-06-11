import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.io.*;

@SuppressWarnings("serial")
public class DBApp implements DBAppInterface, Serializable {
	int counter = 0;

	public DBApp() {
	}

	// this does whatever initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup
	public void init() {
		String path = "src//main//resources//data";
		File f1 = new File(path);
		boolean bool = f1.mkdir();
		if (bool) {
			System.out.println("Folder is created successfully");
		} else {
			System.out.println("Folder Already Created!");
		}
	}

	// following method creates one table only2
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value
	// htblColNameMin and htblColNameMax for passing minimum and maximum values
	// for data in the column. Key is the name of the column
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) throws DBAppException {

		@SuppressWarnings("resource")
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String current = br.readLine();
			while ((current = br.readLine()) != null) {
				String arr[] = current.split(",");
				if (arr[0].equals(strTableName)) {
					throw new DBAppException("this table already exists");
				}
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		int maxTuples = 0;
		int bucketSizeList = 0;
		@SuppressWarnings("unused")
		Hashtable<String, String> h1 = htblColNameType;
		Hashtable<String, String> h2 = htblColNameMin;
		Hashtable<String, String> h3 = htblColNameMax;

		// reading the config file :(
		try (FileReader reader = new FileReader("src/main/resources/DBApp.config")) {
			Properties properties = new Properties();
			properties.load(reader);
			maxTuples = Integer.parseInt(properties.getProperty("MaximumRowsCountinPage"));
			bucketSizeList = Integer.parseInt(properties.getProperty("MaximumKeysCountinIndexBucket"));
		} catch (Exception e) {
			e.printStackTrace();
		}

		Table t1 = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax,
				maxTuples, bucketSizeList, 0);
		try {
			FileWriter fileWritter = new FileWriter("src//main//resources//metadata.csv", true);
			BufferedWriter bw = new BufferedWriter(fileWritter);
			for (int i = 0; i < htblColNameType.size(); i++) {
				if (t1.getColumnsName().get(i).equals(strClusteringKeyColumn)) {
					t1.clusteringKey = true;
					t1.setPrimaryKey(t1.getColumnsName().get(i));
					t1.setPKindex(i);
					t1.setPKType(t1.getColumnsType().get(i));
				} else {
					t1.clusteringKey = false;
				}
				bw.write(strTableName + "," + t1.getColumnsName().get(i) + "," + t1.getColumnsType().get(i) + ","
						+ t1.clusteringKey + "," + t1.indexed + "," + h2.get(t1.getColumnsName().get(i) + "") + ","
						+ h3.get(t1.getColumnsName().get(i) + ""));
				bw.newLine();
			}
			bw.close();
			System.out.println("Done");
		} catch (IOException ex) {
			ex.printStackTrace();
		}

		if (t1.getPKType().equals("java.lang.Integer")) {
			t1.setPKs(new Vector<Integer>());
		} else if (t1.getPKType().equals("java.lang.String")) {
			t1.setPKs(new Vector<String>());
		} else if (t1.getPKType().equals("java.lang.Double")) {
			t1.setPKs(new Vector<Double>());
		} else if (t1.getPKType().equals("java.util.Date")) {
			t1.setPKs(new Vector<Date>());
		}

		serializeTables(t1);
	}

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
					// bw.newLine();
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

	@SuppressWarnings("unchecked")
	private static int[] insertIntoBucketandPage(Bucket b, Table t, Vector inputedValues, Object primaryKeyValue,
			int overflowCounter, Vector<Object> insertedValues) {
		b.PKs = new Vector();
		b.BucketRow = new Vector<Row>();
		int[] return2 = new int[2];
		Row inputRow = new Row();
		for (Object o : inputedValues) {
			if (o.equals(primaryKeyValue)) {
				b.PKs.add(o);
			}
			inputRow.addvalue(o);
		}
		Collections.sort(b.PKs);
		int index1 = Collections.binarySearch(b.PKs, primaryKeyValue);
		int index = Collections.binarySearch(t.getPKs(), primaryKeyValue);
		System.out.println(index + "index in pk vector");
		if (index < 0)
			index = (index + 1) * (-1);
		int pageNumber = searchPageIndex(t, primaryKeyValue);
		System.out.println(pageNumber + "pagenumberrr");
		int rowIndex = index - (250 * (pageNumber));
		System.out.println(rowIndex + "rowwwwindexx");
		inputRow.addvalue(rowIndex);
		inputRow.addvalue(pageNumber);
		if (b.BucketRow.size() < t.bucketSize) {
			b.BucketRow.add(index1, inputRow);

		} else {
			if (b.OverflowBucket.BucketRow.size() < t.bucketSize) {
				if (b.overFlowNames.isEmpty()) {
					overflowCounter++;
					b.overFlowNames.add(overflowCounter);
					b.OverflowBucket.setBucketName("Overflow " + b.overFlowNames.size());
					b.OverflowBucket.BucketRow.add(index1, inputRow);
				} else {
					b.OverflowBucket.setBucketName("Overflow " + b.overFlowNames.size());
					b.OverflowBucket.BucketRow.add(index1, inputRow);
				}
			} else {
				insertIntoBucketandPage(b.OverflowBucket, t, inputedValues, primaryKeyValue, overflowCounter++,
						insertedValues);
			}
		}
		return2[0] = pageNumber;
		return2[1] = rowIndex;
		return return2;
	}

	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key

	// Update All Grids//
	@SuppressWarnings("unchecked")
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		checkConstraint(strTableName, htblColNameValue);
		boolean check = true;
		try {
			check = checkMinMax(strTableName, htblColNameValue);
		} catch (ClassNotFoundException | IOException | ParseException e1) {
			e1.printStackTrace();
		}
		if (check == false)
			throw new DBAppException("Check Min Max Values");
		Table t1 = null;
		try {
			t1 = deserializeTable(strTableName);
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
		if (!t1.GridIndeces.isEmpty()) {
			System.out.println(t1.GridIndeces.size() + " test2");
			if (t1.GridIndeces.size() == 1) {
				Vector<Object> insertedValues = new Vector<Object>();
				Set<String> set = htblColNameValue.keySet();
				Iterator<String> iterator = set.iterator();
				Vector<Object> insertedColumns = new Vector<Object>();

				while (iterator.hasNext()) {
					insertedColumns.add(iterator.next());
				}

				for (int p = 0; p < insertedColumns.size(); p++) {
					insertedValues.add(htblColNameValue.get(insertedColumns.elementAt(p)));
				}

				for (int y = 0; y < insertedColumns.size(); y++) {
					String test = t1.getColumnsType().get(t1.getColumnsName().indexOf(insertedColumns.get(y)));

					if (!(insertedValues.get(y).getClass().getName().equals(test))) {
						throw new DBAppException("Cannot Insert: Data Type Not Applicable");
					}
				}
				GridIndex gridToBeUsed = t1.GridIndeces.get(0);
				System.out.println(gridToBeUsed + " gridTobeUsed");
				String primaryKey = t1.PrimaryKey;
				boolean flagContainsPk = false;
				if (gridToBeUsed.getIndexes().contains(primaryKey))
					flagContainsPk = true;
				System.out.println(flagContainsPk + " flag");
				if (flagContainsPk) {
					System.out.println("primary key entered state");
					String BucketIndex = "";
					Vector<Object> inputedValues = new Vector<Object>();
					Object primaryKeyValue = null;
					System.out.println(gridToBeUsed.getIndexes() + " getIndexes");
					for (String s : gridToBeUsed.getIndexes()) {
						System.out.println(s + " omarrrrrrrrr");
						if (s.equals(t1.PrimaryKey)) {
							primaryKeyValue = htblColNameValue.get(s);
						}
						Object value = htblColNameValue.get(s);
						System.out.println(value + " value");
						inputedValues.add(value);
						System.out.println(inputedValues + " inputed Values");
						System.out.println(value.getClass() + " Get Class");
						if (value.getClass().getSimpleName().equals("Integer")) {
							String min = t1.min.get(s);
							String max = t1.max.get(s);
							int i = intRange(Integer.parseInt(min), Integer.parseInt(max), (int) value);
							BucketIndex += i;
							System.out.println(BucketIndex + "BucketIndex------------");
						} else if (value.getClass().getSimpleName().equals("String")) {
							System.out.println("kamal is here");
							if (s.equals("id")) {
								String min = t1.min.get(s);
								String max = t1.max.get(s);

								String[] parts = min.split("-");
								String minBefore = parts[0]; // 004
								String minAfter = parts[1];

								parts = max.split("-");
								String maxBefore = parts[0]; // 004
								String maxAfter = parts[1];

								parts = ((String) value).split("-");
								String inputBefore = parts[0]; // 004
								String inputAfter = parts[1];

								int i = idRange(Integer.parseInt(minBefore), Integer.parseInt(maxBefore),
										Integer.parseInt(minAfter), Integer.parseInt(maxAfter),
										Integer.parseInt(inputBefore), Integer.parseInt(inputAfter));
								BucketIndex += i;
								System.out.println(BucketIndex + "BcketIndex------------");
							} else {
								System.out.println("omar is here");
								int i = stringRange((String) value);
								BucketIndex += i;
								System.out.println(BucketIndex + "BucketIndex------------");
							}
						} else if (value.getClass().getSimpleName().equals("Double")) {
							String min = t1.min.get(s);
							String max = t1.max.get(s);
							int i = doubleRange(Double.parseDouble(min), Double.parseDouble(max), (double) value);
							BucketIndex += i;
							System.out.println(BucketIndex + "BucketIndex------------");

						} else if (value.getClass().getSimpleName().equals("Date")) {
							String minDate = t1.min.get(s);
							String maxDate = t1.max.get(s);

							String[] parts = minDate.split("-");
							String minYear = parts[0]; // 004
							String minMonth = parts[1];
							String minDay = parts[2];

							parts = maxDate.split("-");
							String maxYear = parts[0]; // 004
							String maxMonth = parts[1];
							String maxDay = parts[2];

							parts = ((String) value).split("-");
							String inputYear = parts[0]; // 004
							String inputMonth = parts[1];
							String inputDay = parts[2];

							int i = DateRange(Integer.parseInt(minYear), Integer.parseInt(maxYear),
									Integer.parseInt(minMonth), Integer.parseInt(maxMonth), Integer.parseInt(minDay),
									Integer.parseInt(maxDay), Integer.parseInt(inputYear), Integer.parseInt(inputMonth),
									Integer.parseInt(inputDay));
							BucketIndex += i;
							System.out.println(BucketIndex + "BucketIndex------------");
						}
					}
					System.out.println(BucketIndex + " bucket Index HEYYEYYEYEY");
					System.out.println(gridToBeUsed.getBucketNames());
					if (gridToBeUsed.getBucketNames().contains(BucketIndex)) {
						System.out.println("if");
						Bucket bucket = null;
						try {
							bucket = deserializeBucket(t1, BucketIndex, gridToBeUsed.getName());
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}

						while (iterator.hasNext()) {
							insertedColumns.add(iterator.next());
						}

//						for (int p = 0; p < insertedColumns.size(); p++) {
//							insertedValues.add(htblColNameValue.get(insertedColumns.elementAt(p)));
//						}

						int[] array = insertIntoBucketandPage(bucket, t1, inputedValues, primaryKeyValue, 0,
								insertedValues);
						for (int x : array) {
							System.out.println(x + " this is array ");
						}
						Page page = null;
						try {
							page = deserializePages(t1, array[0]);
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						} catch (IOException e) {
							Page temp = new Page();
							Row r = new Row();
							r.setValues(insertedValues);
							temp.Rows.add(array[1], r);
							t1.getPKs().add(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())));
							Collections.sort(t1.getPKs());
							serializePages(temp, t1, array[0]);
							serializeBuckets(bucket, t1, gridToBeUsed.getName());
							gridToBeUsed.getBucketNames().add(BucketIndex);
							Collections.sort(gridToBeUsed.getBucketNames());
							t1.getPageNames().add(t1.getName() + "Page" + (t1.getPageNames().size()));
							System.out.println(t1.getPageNames() + " pages NAme");

							return;
						}
						Row r = new Row();
						r.setValues(insertedValues);
						if (t1.getPKs().contains(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())))) {
							serializePages(page, t1, array[0]);
							serializeBuckets(bucket, t1, gridToBeUsed.getName());
							gridToBeUsed.getBucketNames().add(BucketIndex);
							Collections.sort(gridToBeUsed.getBucketNames());
						} else {
							page.Rows.add(array[1], r);
							t1.getPKs().add(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())));
							Collections.sort(t1.getPKs());
							serializePages(page, t1, array[0]);
							serializeBuckets(bucket, t1, gridToBeUsed.getName());
							gridToBeUsed.getBucketNames().add(BucketIndex);
							Collections.sort(gridToBeUsed.getBucketNames());
						}
						System.out.println("end if");
					} else {
						System.out.println("else");
						Bucket bucket = new Bucket();
						bucket.setBucketName(BucketIndex);
						System.out.println(bucket + " 1st bucket");
						while (iterator.hasNext()) {
							insertedColumns.add(iterator.next());
						}

						int[] array = insertIntoBucketandPage(bucket, t1, inputedValues, primaryKeyValue, 0,
								insertedValues);
						for (int i : array) {
							System.out.println(i + " hellozzzzzzzzz ");
						}
						Page page = null;
						try {
							page = deserializePages(t1, array[0]);
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						} catch (IOException e) {
							Page temp = new Page();
							Row r = new Row();
							r.setValues(insertedValues);
							temp.Rows.add(array[1], r);
							t1.getPKs().add(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())));
							Collections.sort(t1.getPKs());
							serializePages(temp, t1, array[0]);
							serializeBuckets(bucket, t1, gridToBeUsed.getName());
							gridToBeUsed.getBucketNames().add(BucketIndex);
							Collections.sort(gridToBeUsed.getBucketNames());
							t1.getPageNames().add(t1.getName() + "Page" + (t1.getPageNames().size()));
							System.out.println(t1.getPageNames() + " pages NAme");
							serializeTables(t1);
							return;
						}
						Row r = new Row();
						r.setValues(insertedValues);
						if (t1.getPKs().contains(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())))) {
							serializePages(page, t1, array[0]);
							serializeBuckets(bucket, t1, gridToBeUsed.getName());
							gridToBeUsed.getBucketNames().add(BucketIndex);
							Collections.sort(gridToBeUsed.getBucketNames());
						} else {
							page.Rows.add(array[1], r);
							t1.getPKs().add(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())));
							Collections.sort(t1.getPKs());
							serializePages(page, t1, array[0]);
							serializeBuckets(bucket, t1, gridToBeUsed.getName());
							gridToBeUsed.getBucketNames().add(BucketIndex);
							Collections.sort(gridToBeUsed.getBucketNames());
						}
						System.out.println("end else");
					}
					serializeTables(t1);
				} else if (!flagContainsPk) {
					if (t1.getPageNames().size() == 0) {
						Set<String> set2 = t1.max.keySet();
						Set<String> set3 = t1.min.keySet();
						Iterator<String> iterator2 = set2.iterator();
						Vector<Object> vectorOfMax = new Vector<Object>();
						while (iterator2.hasNext()) {
							vectorOfMax.add(iterator2.next());
						}
						Iterator<String> iterator3 = set3.iterator();
						Vector<Object> vectorOfMin = new Vector<Object>();
						while (iterator3.hasNext()) {
							vectorOfMin.add(iterator3.next());
						}
						Page temp = new Page();
						temp.Rows.add(new Row());
						temp.getRow().get(0).setValues(insertedValues);

						t1.getPKs().add(temp.getRow().get(0).getValues().get(t1.getPKindex()));
						Collections.sort(t1.getPKs());
						t1.getPageNames().add(t1.getName() + "Page" + 0);

						serializePages(temp, t1, 0);
						serializeTables(t1);
					} else {
						System.out.println(counter++);
						binarySearchInsert(t1, htblColNameValue);
					}
					Vector inputedValues = new Vector();
					for (GridIndex g : t1.GridIndeces) {
						for (String s : g.getIndexes()) {
							// Object value = htblColNameValue.get(s);
							Object o = (insertedValues.get(insertedColumns.indexOf(s)));
							inputedValues.add(o);
							String BucketIndex = "";
							if (o.getClass().equals("java.lang.String")) {
								if (s.equals("id")) {
									String min = t1.min.get(s);
									String max = t1.max.get(s);

									String[] parts = min.split("-");
									String minBefore = parts[0]; // 004
									String minAfter = parts[1];

									parts = max.split("-");
									String maxBefore = parts[0]; // 004
									String maxAfter = parts[1];

									parts = ((String) o).split("-");
									String inputBefore = parts[0]; // 004
									String inputAfter = parts[1];

									int i = idRange(Integer.parseInt(minBefore), Integer.parseInt(maxBefore),
											Integer.parseInt(minAfter), Integer.parseInt(maxAfter),
											Integer.parseInt(inputBefore), Integer.parseInt(inputAfter));
									BucketIndex += i;
									System.out.println(BucketIndex + "BcketIndex------------");
								} else {
									int i = stringRange((String) o);
									BucketIndex += i;
									System.out.println(BucketIndex + "BucketIndex------------");
								}
							} else if (o.getClass().equals("java.lang.Double")) {
								String min = t1.min.get(s);
								String max = t1.max.get(s);
								int i = doubleRange(Double.parseDouble(min), Double.parseDouble(max), (double) o);
								BucketIndex += i;
								System.out.println(BucketIndex + "BucketIndex------------");

							} else if (o.getClass().equals("java.util.Date")) {
								String minDate = t1.min.get(s);
								String maxDate = t1.max.get(s);

								String[] parts = minDate.split("-");
								String minYear = parts[0]; // 004
								String minMonth = parts[1];
								String minDay = parts[2];

								parts = maxDate.split("-");
								String maxYear = parts[0]; // 004
								String maxMonth = parts[1];
								String maxDay = parts[2];

								parts = ((String) o).split("-");
								String inputYear = parts[0]; // 004
								String inputMonth = parts[1];
								String inputDay = parts[2];

								int i = DateRange(Integer.parseInt(minYear), Integer.parseInt(maxYear),
										Integer.parseInt(minMonth), Integer.parseInt(maxMonth),
										Integer.parseInt(minDay), Integer.parseInt(maxDay), Integer.parseInt(inputYear),
										Integer.parseInt(inputMonth), Integer.parseInt(inputDay));
								BucketIndex += i;
								System.out.println(BucketIndex + "BucketIndex------------");
							}
							Object primaryKeyValue = htblColNameValue.get(t1.PrimaryKey);
							if (gridToBeUsed.getBucketNames().contains(BucketIndex)) {
								Bucket bucket = null;
								try {
									bucket = deserializeBucket(t1, BucketIndex, gridToBeUsed.getName());
								} catch (ClassNotFoundException e) {
									e.printStackTrace();
								} catch (IOException e) {
									e.printStackTrace();
								}

								while (iterator.hasNext()) {
									insertedColumns.add(iterator.next());
								}

								for (int p = 0; p < insertedColumns.size(); p++) {
									insertedValues.add(htblColNameValue.get(insertedColumns.elementAt(p)));
								}

								int[] array = insertIntoBucketandPage(bucket, t1, inputedValues, primaryKeyValue, 0,
										insertedValues);
								Page page = null;
								try {
									page = deserializePages(t1, array[0]);
								} catch (ClassNotFoundException e) {
									e.printStackTrace();
								} catch (IOException e) {
									Page temp = new Page();
									Row r = new Row();
									r.setValues(insertedValues);
									temp.Rows.add(array[1], r);
									t1.getPKs().add(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())));
									Collections.sort(t1.getPKs());
									serializePages(temp, t1, array[0]);
									serializeBuckets(bucket, t1, gridToBeUsed.getName());
									gridToBeUsed.getBucketNames().add(BucketIndex);
									Collections.sort(gridToBeUsed.getBucketNames());
									t1.getPageNames().add(t1.getName() + "Page" + (t1.getPageNames().size()));
									System.out.println(t1.getPageNames() + " pages NAme");

									return;
								}
								Row r = new Row();
								r.setValues(insertedValues);
								if (t1.getPKs()
										.contains(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())))) {
									serializePages(page, t1, array[0]);
									serializeBuckets(bucket, t1, gridToBeUsed.getName());
									gridToBeUsed.getBucketNames().add(BucketIndex);
									Collections.sort(gridToBeUsed.getBucketNames());
								} else {
									page.Rows.add(array[1], r);
									t1.getPKs().add(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())));
									Collections.sort(t1.getPKs());
									serializePages(page, t1, array[0]);
									serializeBuckets(bucket, t1, gridToBeUsed.getName());
									gridToBeUsed.getBucketNames().add(BucketIndex);
									Collections.sort(gridToBeUsed.getBucketNames());
								}
							} else {
								Bucket bucket = new Bucket();
								bucket.BucketName = BucketIndex;
								while (iterator.hasNext()) {
									insertedColumns.add(iterator.next());
								}

								for (int p = 0; p < insertedColumns.size(); p++) {
									insertedValues.add(htblColNameValue.get(insertedColumns.elementAt(p)));
								}

								int[] array = insertIntoBucketandPage(bucket, t1, inputedValues, primaryKeyValue, 0,
										insertedValues);
								Page page = null;
								try {
									page = deserializePages(t1, array[0]);
								} catch (ClassNotFoundException e) {
									e.printStackTrace();
								} catch (IOException e) {
									Page temp = new Page();
									Row r = new Row();
									r.setValues(insertedValues);
									temp.Rows.add(array[1], r);
									t1.getPKs().add(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())));
									Collections.sort(t1.getPKs());
									serializePages(temp, t1, array[0]);
									serializeBuckets(bucket, t1, gridToBeUsed.getName());
									gridToBeUsed.getBucketNames().add(BucketIndex);
									Collections.sort(gridToBeUsed.getBucketNames());
									t1.getPageNames().add(t1.getName() + "Page" + (t1.getPageNames().size()));
									System.out.println(t1.getPageNames() + " pages NAme");
									return;
								}
								Row r = new Row();
								r.setValues(insertedValues);
								if (t1.getPKs()
										.contains(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())))) {
									serializePages(page, t1, array[0]);
									serializeBuckets(bucket, t1, gridToBeUsed.getName());
									gridToBeUsed.getBucketNames().add(BucketIndex);
									Collections.sort(gridToBeUsed.getBucketNames());
								} else {
									page.Rows.add(array[1], r);
									t1.getPKs().add(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())));
									Collections.sort(t1.getPKs());
									serializePages(page, t1, array[0]);
									serializeBuckets(bucket, t1, gridToBeUsed.getName());
									gridToBeUsed.getBucketNames().add(BucketIndex);
									Collections.sort(gridToBeUsed.getBucketNames());
								}
							}
						}
					}
				}

			} else if (t1.GridIndeces.size() > 1) {
				System.out.println("entered grid el sa7");
				for (GridIndex g : t1.GridIndeces) {
					System.out.println(g.getName() + " g.getNameeeeee");
					GridIndex gridToBeUsed = g;
					String primaryKey = t1.PrimaryKey;
					boolean flagContainsPk = false;
					if (gridToBeUsed.getIndexes().contains(primaryKey))
						flagContainsPk = true;
					if (flagContainsPk) {
						String BucketIndex = "";
						@SuppressWarnings("rawtypes")
						Vector inputedValues = new Vector();
						Object primaryKeyValue = null;
						for (String s : gridToBeUsed.getIndexes()) {

							if (s.equals(t1.PrimaryKey)) {
								primaryKeyValue = htblColNameValue.get(s);
							}
							Object value = htblColNameValue.get(s);
							inputedValues.add(value);
							if (value.getClass().getSimpleName().equals("Integer")) {
								String min = t1.min.get(s);
								String max = t1.max.get(s);
								int i = intRange(Integer.parseInt(min), Integer.parseInt(max), (int) value);
								BucketIndex += i;
								System.out.println(BucketIndex + "BucketIndex------------");
							} else if (value.getClass().getSimpleName().equals("String")) {
								if (s.equals("id")) {
									String min = t1.min.get(s);
									String max = t1.max.get(s);

									String[] parts = min.split("-");
									String minBefore = parts[0]; // 004
									String minAfter = parts[1];

									parts = max.split("-");
									String maxBefore = parts[0]; // 004
									String maxAfter = parts[1];

									parts = ((String) value).split("-");
									String inputBefore = parts[0]; // 004
									String inputAfter = parts[1];

									int i = idRange(Integer.parseInt(minBefore), Integer.parseInt(maxBefore),
											Integer.parseInt(minAfter), Integer.parseInt(maxAfter),
											Integer.parseInt(inputBefore), Integer.parseInt(inputAfter));
									BucketIndex += i;
									System.out.println(BucketIndex + "BcketIndex------------");
								} else {
									int i = stringRange((String) value);
									BucketIndex += i;
									System.out.println(BucketIndex + "BucketIndex------------");
								}
							} else if (value.getClass().getSimpleName().equals("Double")) {
								String min = t1.min.get(s);
								String max = t1.max.get(s);
								int i = doubleRange(Double.parseDouble(min), Double.parseDouble(max), (double) value);
								BucketIndex += i;
								System.out.println(BucketIndex + "BucketIndex------------");

							} else if (value.getClass().getSimpleName().equals("Date")) {
								String minDate = t1.min.get(s);
								String maxDate = t1.max.get(s);

								String[] parts = minDate.split("-");
								String minYear = parts[0]; // 004
								String minMonth = parts[1];
								String minDay = parts[2];

								parts = maxDate.split("-");
								String maxYear = parts[0]; // 004
								String maxMonth = parts[1];
								String maxDay = parts[2];

								parts = ((String) value).split("-");
								String inputYear = parts[0]; // 004
								String inputMonth = parts[1];
								String inputDay = parts[2];

								int i = DateRange(Integer.parseInt(minYear), Integer.parseInt(maxYear),
										Integer.parseInt(minMonth), Integer.parseInt(maxMonth),
										Integer.parseInt(minDay), Integer.parseInt(maxDay), Integer.parseInt(inputYear),
										Integer.parseInt(inputMonth), Integer.parseInt(inputDay));
								BucketIndex += i;
								System.out.println(BucketIndex + "BucketIndex------------");
							}
						}
						if (gridToBeUsed.getBucketNames().contains(BucketIndex)) {
							Bucket bucket = null;
							try {
								bucket = deserializeBucket(t1, BucketIndex, gridToBeUsed.getName());
							} catch (ClassNotFoundException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
							Vector<Object> insertedValues = new Vector<Object>();
							Set<String> set = htblColNameValue.keySet();
							Iterator<String> iterator = set.iterator();
							Vector<Object> insertedColumns = new Vector<Object>();

							while (iterator.hasNext()) {
								insertedColumns.add(iterator.next());
							}

							for (int p = 0; p < insertedColumns.size(); p++) {
								insertedValues.add(htblColNameValue.get(insertedColumns.elementAt(p)));
							}

							int[] array = insertIntoBucketandPage(bucket, t1, inputedValues, primaryKeyValue, 0,
									insertedValues);
							Page page = null;
							try {
								page = deserializePages(t1, array[0]);
							} catch (ClassNotFoundException e) {
								e.printStackTrace();
							} catch (IOException e) {
								Page temp = new Page();
								Row r = new Row();
								r.setValues(insertedValues);
								temp.Rows.add(array[1], r);
								t1.getPKs().add(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())));
								Collections.sort(t1.getPKs());
								serializePages(temp, t1, array[0]);
								serializeBuckets(bucket, t1, gridToBeUsed.getName());
								gridToBeUsed.getBucketNames().add(BucketIndex);
								Collections.sort(gridToBeUsed.getBucketNames());
								t1.getPageNames().add(t1.getName() + "Page" + (t1.getPageNames().size()));
								continue;
							}
							Row r = new Row();
							r.setValues(insertedValues);
							System.out.println(insertedValues + " inserted values");
							System.out.println(
									r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())));

							if (t1.getPKs()
									.contains(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())))) {
								serializePages(page, t1, array[0]);
								serializeBuckets(bucket, t1, gridToBeUsed.getName());
								gridToBeUsed.getBucketNames().add(BucketIndex);
								Collections.sort(gridToBeUsed.getBucketNames());
							} else {
								page.Rows.add(array[1], r);
								t1.getPKs().add(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())));
								Collections.sort(t1.getPKs());
								serializePages(page, t1, array[0]);
								serializeBuckets(bucket, t1, gridToBeUsed.getName());
								gridToBeUsed.getBucketNames().add(BucketIndex);
								Collections.sort(gridToBeUsed.getBucketNames());
							}
						} else {
							Bucket bucket = new Bucket();
							bucket.BucketName = BucketIndex;
							Vector<Object> insertedValues = new Vector<Object>();
							Set<String> set = htblColNameValue.keySet();
							Iterator<String> iterator = set.iterator();
							Vector<Object> insertedColumns = new Vector<Object>();

							while (iterator.hasNext()) {
								insertedColumns.add(iterator.next());
							}

							for (int p = 0; p < insertedColumns.size(); p++) {
								insertedValues.add(htblColNameValue.get(insertedColumns.elementAt(p)));
							}

							int[] array = insertIntoBucketandPage(bucket, t1, inputedValues, primaryKeyValue, 0,
									insertedValues);
							Page page = null;
							for (int x : array) {
								System.out.println(x);
							}
							System.out.println(insertedValues + "hhhhhhh");
							try {
								page = deserializePages(t1, array[0]);
							} catch (ClassNotFoundException e) {
								e.printStackTrace();
							} catch (IOException e) {
								Page temp = new Page();
								Row r = new Row();
								r.setValues(insertedValues);
								temp.Rows.add(array[1], r);
								t1.getPKs().add(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())));
								Collections.sort(t1.getPKs());
								serializePages(temp, t1, array[0]);
								serializeBuckets(bucket, t1, gridToBeUsed.getName());
								gridToBeUsed.getBucketNames().add(BucketIndex);
								Collections.sort(gridToBeUsed.getBucketNames());
								t1.getPageNames().add(t1.getName() + "Page" + (t1.getPageNames().size()));
								continue;
							}
							Row r = new Row();
							r.setValues(insertedValues);
							if (t1.getPKs()
									.contains(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())))) {
								serializePages(page, t1, array[0]);
								serializeBuckets(bucket, t1, gridToBeUsed.getName());
								gridToBeUsed.getBucketNames().add(BucketIndex);
								Collections.sort(gridToBeUsed.getBucketNames());
							} else {
								page.Rows.add(array[1], r);
								t1.getPKs().add(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())));
								Collections.sort(t1.getPKs());
								serializePages(page, t1, array[0]);
								serializeBuckets(bucket, t1, gridToBeUsed.getName());
								gridToBeUsed.getBucketNames().add(BucketIndex);
								Collections.sort(gridToBeUsed.getBucketNames());
							}
						}
					} else {
						// TODO partial query
					}
				}
			} else {
				Vector<Object> insertedValues = new Vector<Object>();
				Set<String> set = htblColNameValue.keySet();
				Iterator<String> iterator = set.iterator();
				Vector<Object> insertedColumns = new Vector<Object>();

				while (iterator.hasNext()) {
					insertedColumns.add(iterator.next());
				}

				for (int p = 0; p < insertedColumns.size(); p++) {
					insertedValues.add(htblColNameValue.get(insertedColumns.elementAt(p)));
				}

				for (int y = 0; y < insertedColumns.size(); y++) {
					String test = t1.getColumnsType().get(t1.getColumnsName().indexOf(insertedColumns.get(y)));

					if (!(insertedValues.get(y).getClass().getName().equals(test))) {
						throw new DBAppException("Cannot Insert: Data Type Not Applicable");
					}
				}
				if (t1.getPageNames().size() == 0) {
					Set<String> set2 = t1.max.keySet();
					Set<String> set3 = t1.min.keySet();
					Iterator<String> iterator2 = set2.iterator();
					Vector<Object> vectorOfMax = new Vector<Object>();
					while (iterator2.hasNext()) {
						vectorOfMax.add(iterator2.next());
					}
					Iterator<String> iterator3 = set3.iterator();
					Vector<Object> vectorOfMin = new Vector<Object>();
					while (iterator3.hasNext()) {
						vectorOfMin.add(iterator3.next());
					}
					Page temp = new Page();
					temp.Rows.add(new Row());
					temp.getRow().get(0).setValues(insertedValues);

					t1.getPKs().add(temp.getRow().get(0).getValues().get(t1.getPKindex()));
					Collections.sort(t1.getPKs());
					t1.getPageNames().add(t1.getName() + "Page" + 0);

					serializePages(temp, t1, 0);
					serializeTables(t1);
				} else {
					System.out.println(counter++);
					binarySearchInsert(t1, htblColNameValue);
				}

				@SuppressWarnings("rawtypes")
				Vector inputedValues = new Vector();
				for (GridIndex g : t1.GridIndeces) {
					for (String s : g.getIndexes()) {
						// Object value = htblColNameValue.get(s);
						Object o = (insertedValues.get(insertedColumns.indexOf(s)));
						inputedValues.add(o);
						String BucketIndex = "";
						if (o.getClass().equals("java.lang.Integer")) {
							String min = t1.min.get(s);
							String max = t1.max.get(s);
							int i = intRange(Integer.parseInt(min), Integer.parseInt(max), Integer.parseInt(s));
							BucketIndex += i;
							System.out.println(BucketIndex + "BucketIndex------------");
						} else if (o.getClass().equals("java.lang.String")) {
							if (s.equals("id")) {
								String min = t1.min.get(s);
								String max = t1.max.get(s);

								String[] parts = min.split("-");
								String minBefore = parts[0]; // 004
								String minAfter = parts[1];

								parts = max.split("-");
								String maxBefore = parts[0]; // 004
								String maxAfter = parts[1];

								parts = ((String) o).split("-");
								String inputBefore = parts[0]; // 004
								String inputAfter = parts[1];

								int i = idRange(Integer.parseInt(minBefore), Integer.parseInt(maxBefore),
										Integer.parseInt(minAfter), Integer.parseInt(maxAfter),
										Integer.parseInt(inputBefore), Integer.parseInt(inputAfter));
								BucketIndex += i;
								System.out.println(BucketIndex + "BcketIndex------------");
							} else {
								int i = stringRange(s);
								BucketIndex += i;
								System.out.println(BucketIndex + "BucketIndex------------");
							}
						} else if (o.getClass().equals("java.lang.Double")) {
							String min = t1.min.get(s);
							String max = t1.max.get(s);
							int i = doubleRange(Double.parseDouble(min), Double.parseDouble(max),
									Double.parseDouble(s));
							BucketIndex += i;
							System.out.println(BucketIndex + "BucketIndex------------");

						} else if (o.getClass().equals("java.util.Date")) {
							String minDate = t1.min.get(s);
							String maxDate = t1.max.get(s);

							String[] parts = minDate.split("-");
							String minYear = parts[0]; // 004
							String minMonth = parts[1];
							String minDay = parts[2];

							parts = maxDate.split("-");
							String maxYear = parts[0]; // 004
							String maxMonth = parts[1];
							String maxDay = parts[2];

							parts = s.split("-");
							String inputYear = parts[0]; // 004
							String inputMonth = parts[1];
							String inputDay = parts[2];

							int i = DateRange(Integer.parseInt(minYear), Integer.parseInt(maxYear),
									Integer.parseInt(minMonth), Integer.parseInt(maxMonth), Integer.parseInt(minDay),
									Integer.parseInt(maxDay), Integer.parseInt(inputYear), Integer.parseInt(inputMonth),
									Integer.parseInt(inputDay));
							BucketIndex += i;
							System.out.println(BucketIndex + "BucketIndex------------");
						}
						Object primaryKeyValue = htblColNameValue.get(t1.PrimaryKey);

						if (g.getBucketNames().contains(BucketIndex)) {
							Bucket bucket = null;
							try {
								bucket = deserializeBucket(t1, BucketIndex, g.getName());
							} catch (ClassNotFoundException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}

							while (iterator.hasNext()) {
								insertedColumns.add(iterator.next());
							}

							for (int p = 0; p < insertedColumns.size(); p++) {
								insertedValues.add(htblColNameValue.get(insertedColumns.elementAt(p)));
							}

							int[] array = insertIntoBucketandPage(bucket, t1, inputedValues, primaryKeyValue, 0,
									insertedValues);
							Page page = null;
							try {
								page = deserializePages(t1, array[0]);
							} catch (ClassNotFoundException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
							Row r = new Row();
							r.setValues(insertedValues);
							t1.getPKs().add(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())));
							Collections.sort(t1.getPKs());
							serializePages(page, t1, array[0]);
							serializeBuckets(bucket, t1, g.getName());
						} else {
							Bucket bucket = new Bucket();

							while (iterator.hasNext()) {
								insertedColumns.add(iterator.next());
							}

							for (int p = 0; p < insertedColumns.size(); p++) {
								insertedValues.add(htblColNameValue.get(insertedColumns.elementAt(p)));
							}

							int[] array = insertIntoBucketandPage(bucket, t1, inputedValues, primaryKeyValue, 0,
									insertedValues);
							Page page = null;
							try {
								page = deserializePages(t1, array[0]);
							} catch (ClassNotFoundException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
							Row r = new Row();
							r.setValues(insertedValues);
							t1.getPKs().add(r.getValues().get(t1.getColumnsName().indexOf(t1.getPrimaryKey())));
							Collections.sort(t1.getPKs());
							serializePages(page, t1, array[0]);
							serializeBuckets(bucket, t1, g.getName());
							g.getBucketNames().add(BucketIndex);
							Collections.sort(g.getBucketNames());
						}
					}
				}
			}
			serializeTables(t1);
		} else {
			Vector<Object> insertedValues = new Vector<Object>();
			Set<String> set = htblColNameValue.keySet();
			Iterator<String> iterator = set.iterator();
			Vector<Object> insertedColumns = new Vector<Object>();

			while (iterator.hasNext()) {
				insertedColumns.add(iterator.next());
			}

			for (int p = 0; p < insertedColumns.size(); p++) {
				insertedValues.add(htblColNameValue.get(insertedColumns.elementAt(p)));
			}

			for (int y = 0; y < insertedColumns.size(); y++) {
				String test = t1.getColumnsType().get(t1.getColumnsName().indexOf(insertedColumns.get(y)));

				if (!(insertedValues.get(y).getClass().getName().equals(test))) {
					throw new DBAppException("Cannot Insert: Data Type Not Applicable");
				}
			}
			if (t1.getPageNames().size() == 0) {
				Set<String> set2 = t1.max.keySet();
				Set<String> set3 = t1.min.keySet();
				Iterator<String> iterator2 = set2.iterator();
				Vector<Object> vectorOfMax = new Vector<Object>();
				while (iterator2.hasNext()) {
					vectorOfMax.add(iterator2.next());
				}
				Iterator<String> iterator3 = set3.iterator();
				Vector<Object> vectorOfMin = new Vector<Object>();
				while (iterator3.hasNext()) {
					vectorOfMin.add(iterator3.next());
				}
				Page temp = new Page();
				temp.Rows.add(new Row());
				temp.getRow().get(0).setValues(insertedValues);

				t1.getPKs().add(temp.getRow().get(0).getValues().get(t1.getPKindex()));
				Collections.sort(t1.getPKs());
				t1.getPageNames().add(t1.getName() + "Page" + 0);

				serializePages(temp, t1, 0);
				serializeTables(t1);
			} else {
				System.out.println(counter++);
				binarySearchInsert(t1, htblColNameValue);
			}
		}
	}

	@SuppressWarnings("unchecked")
	public static void binarySearchInsert(Table t1, Hashtable<String, Object> htblColNameValue) throws DBAppException {

		Page temp = null;
		try {
			temp = deserializePages(t1, 0);
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
		String tempo = t1.PrimaryKey;
		Object obj = htblColNameValue.get(tempo);
		int index = Collections.binarySearch(t1.getPKs(), obj);
		if (index >= 0) {
			throw new DBAppException("There is a row with that Primary Key");
		} else {

			if (t1.getPageNames().size() == 1) {
				if (temp.getRow().size() < 250) {
					Vector<Object> insertedValues = new Vector<Object>();
					Set<String> set = htblColNameValue.keySet();
					Iterator<String> iterator = set.iterator();
					Vector<Object> insertedColumns = new Vector<Object>();

					while (iterator.hasNext()) {
						insertedColumns.add(iterator.next());
					}

					for (int p = 0; p < insertedColumns.size(); p++) {
						insertedValues.add(htblColNameValue.get(insertedColumns.elementAt(p)));
					}

					for (int y = 0; y < insertedColumns.size(); y++) {
						String test = t1.getColumnsType().get(t1.getColumnsName().indexOf(insertedColumns.get(y)));
						if (!(insertedValues.get(y).getClass().getName().equals(test))) {
							throw new DBAppException("Cannot Insert: Data Type Not Applicable");
						}
					}

					Set<String> set2 = t1.max.keySet();
					Set<String> set3 = t1.min.keySet();
					Iterator<String> iterator2 = set2.iterator();
					Vector<Object> vectorOfMax = new Vector<Object>();
					while (iterator2.hasNext()) {
						vectorOfMax.add(iterator2.next());
					}
					Iterator<String> iterator3 = set3.iterator();
					Vector<Object> vectorOfMin = new Vector<Object>();
					while (iterator3.hasNext()) {
						vectorOfMin.add(iterator3.next());
					}
					boolean flag3 = true;
					if (flag3) {
						Collections.sort(t1.getPKs());
						String tempo1 = t1.PrimaryKey;
						Object obj1 = htblColNameValue.get(tempo1);
						int index1 = Collections.binarySearch(t1.getPKs(), obj1);
						if (index1 < 0)
							index1 = (index1 + 1) * (-1);
						t1.getPKs().add(index1, obj1);
						Row r = new Row();
						r.setValues(insertedValues);
						temp.Rows.add(index1, r);
						serializePages(temp, t1, 0);
					}
				} else if (temp.getRow().size() == 250) {
					Vector<Object> insertedValues = new Vector<Object>();
					Set<String> set = htblColNameValue.keySet();
					Iterator<String> iterator = set.iterator();
					Vector<Object> insertedColumns = new Vector<Object>();

					while (iterator.hasNext()) {
						insertedColumns.add(iterator.next());
					}

					for (int p = 0; p < insertedColumns.size(); p++) {
						insertedValues.add(htblColNameValue.get(insertedColumns.elementAt(p)));
					}

					for (int y = 0; y < insertedColumns.size(); y++) {
						String test = t1.getColumnsType().get(t1.getColumnsName().indexOf(insertedColumns.get(y)));
						if (!(insertedValues.get(y).getClass().getName().equals(test))) {
							throw new DBAppException("Cannot Insert: Data Type Not Applicable");
						}
					}

					Set<String> set2 = t1.max.keySet();
					Set<String> set3 = t1.min.keySet();
					Iterator<String> iterator2 = set2.iterator();
					Vector<Object> vectorOfMax = new Vector<Object>();
					while (iterator2.hasNext()) {
						vectorOfMax.add(iterator2.next());
					}
					Iterator<String> iterator3 = set3.iterator();
					Vector<Object> vectorOfMin = new Vector<Object>();
					while (iterator3.hasNext()) {
						vectorOfMin.add(iterator3.next());
					}
					Collections.sort(t1.getPKs());
					String tempo1 = t1.PrimaryKey;
					Object obj1 = htblColNameValue.get(tempo1);
					int index1 = Collections.binarySearch(t1.getPKs(), obj1);
					if (index1 < 0)
						index1 = (index1 + 1) * (-1);
					if (index1 >= 250) {
						Page temp1 = new Page();
						Row r = new Row();
						r.setValues(insertedValues);
						temp1.Rows.add(index1 - 250, r);
						t1.getPageNames().add(t1.getName() + "Page" + 1);
						t1.getPKs().add(index1, obj1);
						serializePages(temp, t1, 0);
						serializePages(temp1, t1, 1);
						serializeTables(t1);
					} else {
						if (index1 < 250) {
							Row r = new Row();
							r.setValues(insertedValues);
							Page temp1 = new Page();
							temp.Rows.add(index1, r);
							temp1.Rows.add(temp.Rows.remove(249));
							t1.getPageNames().add(t1.getName() + "Page" + 1);
							t1.getPKs().add(index1, obj1);
							serializePages(temp, t1, 0);
							serializePages(temp1, t1, 1);
							serializeTables(t1);
						}
					}
				}
			} else if (t1.getPageNames().size() > 1) {
				for (int j1 = 0; j1 < t1.getPageNames().size(); j1++) {
					Page p = null;
					try {
						p = deserializePages(t1, j1);
					} catch (ClassNotFoundException | IOException e) {
						e.printStackTrace();
					}
					p.min = AssignMin(p, t1);
					p.max = AssignMax(p, t1);
					serializePages(p, t1, j1);
				}
				Vector<Object> insertedValues = new Vector<Object>();
				Set<String> set = htblColNameValue.keySet();
				Iterator<String> iterator = set.iterator();
				Vector<Object> insertedColumns = new Vector<Object>();

				while (iterator.hasNext()) {
					insertedColumns.add(iterator.next());
				}

				for (int p = 0; p < insertedColumns.size(); p++) {
					insertedValues.add(htblColNameValue.get(insertedColumns.elementAt(p)));
				}

				for (int y = 0; y < insertedColumns.size(); y++) {
					String test = t1.getColumnsType().get(t1.getColumnsName().indexOf(insertedColumns.get(y)));

					if (!(insertedValues.get(y).getClass().getName().equals(test))) {
						throw new DBAppException("Cannot Insert: Data Type Not Applicable");
					}
				}

				Set<String> set2 = t1.max.keySet();
				Set<String> set3 = t1.min.keySet();
				Iterator<String> iterator2 = set2.iterator();
				Vector<Object> vectorOfMax = new Vector<Object>();
				while (iterator2.hasNext()) {
					vectorOfMax.add(iterator2.next());
				}
				Iterator<String> iterator3 = set3.iterator();
				Vector<Object> vectorOfMin = new Vector<Object>();
				while (iterator3.hasNext()) {
					vectorOfMin.add(iterator3.next());
				}
				Vector<Object> vectorOFmins = new Vector<Object>();
				Vector<Object> vectorOFmaxs = new Vector<Object>();
				int pageOfInsertion = 0;
				Object pkOFrow = htblColNameValue.get(t1.getPrimaryKey());
				for (int i1 = 0; i1 < t1.getPageNames().size(); i1++) {
					Page p = null;
					try {
						p = deserializePages(t1, i1);
					} catch (ClassNotFoundException | IOException e) {
						e.printStackTrace();
					}
					vectorOFmins.add(p.min);
					vectorOFmaxs.add(p.max);

				}
				// FIX COMPARE TO
				for (int x = 0; x < vectorOFmins.size(); x++) {
					if ((pkOFrow.toString()).compareTo(vectorOFmins.get(x).toString()) > 0
							&& (pkOFrow.toString()).compareTo(vectorOFmaxs.get(x).toString()) < 0) {
						pageOfInsertion = x;
					}
				}

				Row insertedRow = new Row();
				insertedRow.setValues(insertedValues);

				try {
					if (deserializePages(t1, pageOfInsertion).getRow().size() == 250) {
						if (deserializePages(t1, pageOfInsertion + 1).getRow().size() == 250) {
							if (t1.overflowNames.size() == 0) {
								Page overflow = new Page();

								Collections.sort(t1.getPksOverflow());
								String tempo1 = t1.PrimaryKey;
								Object obj1 = htblColNameValue.get(tempo1);
								int index1 = Collections.binarySearch(t1.getPksOverflow(), obj1);
								if (index1 < 0)
									index1 = (index1 + 1) * (-1);
								t1.getPksOverflow().add(index1, obj1);

								Page p = deserializePages(t1, pageOfInsertion);
								p.max = obj1;
								serializePages(p, t1, pageOfInsertion);

								overflow.getRow().add(insertedRow);
								t1.overflowNames.add(t1.getName() + "Overflow" + 0);
								serializeOverFlow(overflow, t1, 0);
							} else {
								boolean flag = true;
								for (String s : t1.overflowNames) {
									Page overflowtemp = deserializeOverFlowPage(t1, t1.overflowNames.indexOf(s));
									if (overflowtemp.getRow().size() < 250) {

										Collections.sort(t1.getPksOverflow());
										String tempo1 = t1.PrimaryKey;
										Object obj1 = htblColNameValue.get(tempo1);
										int index1 = Collections.binarySearch(t1.getPksOverflow(), obj1);
										if (index1 < 0)
											index1 = (index1 + 1) * (-1);
										t1.getPksOverflow().add(index1, obj1);

										Page p = deserializePages(t1, pageOfInsertion);
										p.max = obj1;
										serializePages(p, t1, pageOfInsertion);

										overflowtemp.getRow().add(index1, insertedRow);
										serializeOverFlow(overflowtemp, t1, t1.overflowNames.indexOf(s));
										flag = true;
										break;
									} else {
										flag = false;
										continue;
									}
								}
								if (flag == false) {
									Page overflow = new Page();
									Collections.sort(t1.getPksOverflow());
									String tempo1 = t1.PrimaryKey;
									Object obj1 = htblColNameValue.get(tempo1);
									int index1 = Collections.binarySearch(t1.getPksOverflow(), obj1);
									if (index1 < 0)
										index1 = (index1 + 1) * (-1);
									t1.getPksOverflow().add(index1, obj1);

									Page p = deserializePages(t1, pageOfInsertion);
									p.max = obj1;
									serializePages(p, t1, pageOfInsertion);
									overflow.getRow().add(index1, insertedRow);
									serializeOverFlow(overflow, t1, t1.overflowNames.size());
									t1.overflowNames.add(t1.getName() + "Overflow" + t1.overflowNames.size());
								}
							}
						} else if (deserializePages(t1, pageOfInsertion + 1).getRow().size() < 250) {
							Page p = deserializePages(t1, pageOfInsertion + 1);
							Row shiftedRow = deserializePages(t1, pageOfInsertion).Rows.get(249);
							// Object shiftedRowPK =
							// shiftedRow.values.get(t1.getPKindex());
							Object obj1 = insertedRow.getValues().get(t1.getPKindex());
							int index1 = Collections.binarySearch(t1.getPKs(), obj1);
							if (index1 < 0)
								index1 = (index1 + 1) * (-1);
							t1.getPKs().add(index1, obj1);
							// int indexOfinsert =
							// Collections.binarySearch(t1.getPKs(),
							// shiftedRowPK);
							p.Rows.add(0, insertedRow);
							Page t = deserializePages(t1, pageOfInsertion);
							t.getRow().remove(shiftedRow);
							serializePages(t, t1, pageOfInsertion);
							serializePages(p, t1, pageOfInsertion + 1);
						}
					}
				} catch (ClassNotFoundException | IOException e) {
					e.printStackTrace();
				}
			}
			serializeTables(t1);
		}

	}

	// following method creates one index either multidimensional
	// or single dimension depending on the count of column names passed.
	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
		Table correctTable = null;
		try {
			correctTable = deserializeTable(strTableName);
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		if (correctTable == null) {
			throw new DBAppException("Table Not found");
		}

		for (int k = 0; k < correctTable.getKeysWithIndex().size(); k++) {
			correctTable.getKeysWithIndex().set(k, false);
		}

		Vector<String> strarrColNameTemp = new Vector<String>();
		for (String s : strarrColName) {
			if (!(correctTable.getColumnsName().contains(s))) {
				throw new DBAppException("Column " + s + " is not in the table");
			} else {
				strarrColNameTemp.add(s);
				int index = correctTable.getColumnsName().indexOf(s);
				correctTable.getKeysWithIndex().set(index, true);
			}
		}
		int i = 0;
		String concate = "";
		// 3alshan tob2a same order as in columns name not order in input array
		for (boolean t : correctTable.getKeysWithIndex()) {
			if (t == true) {
				concate += correctTable.getColumnsName().get(i) + " ";
			}
			i++;
		}
		if (correctTable.GridNames.contains(concate))
			throw new DBAppException("Index Already Created");

		GridIndex grid = new GridIndex();
		grid.setName(concate);

		i = 0;

		for (boolean t : correctTable.getKeysWithIndex()) {
			if (t == true) {
				grid.getIndexes().add(correctTable.getColumnsName().get(i));
			}
			i++;
		}

		for (int i1 = 0; i1 < strarrColNameTemp.size(); i1++) {
			Vector<String> temp = new Vector<String>(10);
			Hashtable<String, String> max = correctTable.max;
			Hashtable<String, String> min = correctTable.min;
			Vector<String> columnsName = correctTable.columnsName;

			int index = columnsName.indexOf(strarrColNameTemp.get(i1));
			double total_length = 0;
			if (correctTable.getColumnsType().get(index).equals("java.lang.Integer")) {
				int max1 = Integer.parseInt(max.get(strarrColNameTemp.get(i1)));
				int min1 = Integer.parseInt(min.get(strarrColNameTemp.get(i1)));

				int total_length1 = max1 - min1;
				int subrange_length = total_length1 / 10;

				int current_start = min1;
				for (int i11 = 0; i11 < 10; ++i11) {
					System.out.println(
							"Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
					temp.add("Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
					current_start += subrange_length;
				}

			} else if (correctTable.getColumnsType().get(index).equals("java.lang.String")) {

				if (strarrColName[i1].equals("id")) {
					String minID = min.get(strarrColNameTemp.get(i1));
					String maxID = max.get(strarrColNameTemp.get(i1));

					String[] parts = minID.split("-");
					String minBefore = parts[0]; // 004
					String minAfter = parts[1];

					parts = maxID.split("-");
					String maxBefore = parts[0]; // 004
					String maxAfter = parts[1];

					total_length = (Integer.parseInt(maxBefore) - Integer.parseInt(minBefore));
					double subrange_length = (total_length / 10) + 0;

					double current_start = Integer.parseInt(minBefore);

					for (int i11 = 0; i11 < 10; ++i11) {
						if (i11 == 9) {
							current_start -= 9;
						}
						System.out.println("Smaller range: [" + (int) current_start + "-"
								+ String.format("%04d", Integer.parseInt(minAfter)) + ", "
								+ ((int) (current_start + subrange_length)) + "-"
								+ String.format("%04d", Integer.parseInt(maxAfter)) + "]");

						temp.add("Smaller range: [" + (int) current_start + "-"
								+ String.format("%04d", Integer.parseInt(minAfter)) + ", "
								+ ((int) (current_start + subrange_length)) + "-"
								+ String.format("%04d", Integer.parseInt(maxAfter)) + "]");

						current_start += subrange_length + 1;
					}
				} else {
					String min1 = min.get(strarrColNameTemp.get(i1));

					int x1 = 'A';
					int y1 = 'Z';

					double total_length1 = y1 - x1;
					double subrange_length1 = total_length1 / 5;

					for (int i11 = 0; i11 < 5; ++i11) {
						String t = "";
						for (int j = 0; j < min1.length(); j++) {
							if (i11 == 4) {
								int x11 = min1.charAt(j);
								double l = x11 + subrange_length1 - 4;

								t += (char) (l);
							} else {
								int x11 = min1.charAt(j);
								double l = x11 + subrange_length1;

								t += (char) (l);
							}

						}

						System.out.println("Smaller range: [" + min1 + ", " + t + "]");
						temp.add("Smaller range: [" + min1 + ", " + t + "]");
						String m = "";
						for (int j = 0; j < t.length(); j++) {
							int x3 = t.charAt(j);
							m += (char) (x3 + 1);
						}
						min1 = m;
					}

					// System.out.println("Lower");
					int x = 'a';
					int y = 'z';

					double total_length11 = y - x;
					double subrange_length = total_length11 / 5;

					String min2 = min1.toLowerCase();

					for (int i11 = 0; i11 < 5; ++i11) {
						String t = "";
						for (int j = 0; j < min2.length(); j++) {
							if (i11 == 4) {
								int x11 = min2.charAt(j);
								double l = x11 + subrange_length - 4;

								t += (char) (l);
							} else {
								int x11 = min2.charAt(j);
								double l = x11 + subrange_length;

								t += (char) (l);
							}

						}

						System.out.println("Smaller range: [" + min2 + ", " + t + "]");
						temp.add("Smaller range: [" + min2 + ", " + t + "]");
						String m = "";
						for (int j = 0; j < t.length(); j++) {
							int x3 = t.charAt(j);
							m += (char) (x3 + 1);
						}
						min2 = m;
					}
				}

			} else if (correctTable.getColumnsType().get(index).equals("java.lang.Double")) {
				double max1 = Double.parseDouble(max.get(strarrColNameTemp.get(i1)));
				double min1 = Double.parseDouble(min.get(strarrColNameTemp.get(i1)));

				double total_length1 = max1 - min1;
				double subrange_length = total_length1 / 10;

				double current_start = 0.7;
				for (int i11 = 0; i11 < 10; ++i11) {
					System.out.println(
							"Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
					temp.add("Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
					current_start += subrange_length;
				}

			} else if (correctTable.getColumnsType().get(index).equals("java.lang.Date")) {

				String minDate = min.get(strarrColNameTemp.get(i1));
				String maxDate = max.get(strarrColNameTemp.get(i1));

				String[] parts = minDate.split("-");
				String minYear = parts[0]; // 004
				String minMonth = parts[1];
				String minDay = parts[2];

				parts = maxDate.split("-");
				String maxYear = parts[0]; // 004
				String maxMonth = parts[1];
				String maxDay = parts[2];

				double total_length1 = (Integer.parseInt(maxYear) - Integer.parseInt(minYear));
				double subrange_length = (total_length1 / 10) + 0.1;
				double current_start = Integer.parseInt(minYear);

				double total_length11 = (Integer.parseInt(maxMonth) - Integer.parseInt(minMonth));
				double subrange_length1 = (total_length11 / 10) + 0.1;
				double current_start1 = Integer.parseInt(minMonth);

				double total_length2 = (Integer.parseInt(maxDay) - Integer.parseInt(minDay));
				double subrange_length2 = (total_length2 / 10) + 0.1;
				double current_start2 = Integer.parseInt(minDay);

				for (int i11 = 0; i11 < 10; ++i11) {
					System.out.println("Smaller range: [" + (int) current_start + "-" + (int) current_start1 + "-"
							+ (int) current_start2 + ", " + (int) (current_start + subrange_length) + "-"
							+ (int) (current_start1 + subrange_length1) + "-" + (int) current_start2 + "]");
					temp.add("Smaller range: [" + (int) current_start + "-" + (int) current_start1 + "-"
							+ (int) current_start2 + ", " + (int) (current_start + subrange_length) + "-"
							+ (int) (current_start1 + subrange_length1) + "-" + (int) current_start2 + "]");
					current_start += subrange_length;
					current_start1 += subrange_length1;
					current_start2 += subrange_length2;
				}
			}
			grid.getGridIndex().add(temp);
		}
		Vector<String> indexes = new Vector<String>();
		for (String s : strarrColName) {
			indexes.add(s);
		}
		correctTable.GridNames.add(grid.getName());
		correctTable.GridIndeces.add(grid);
		modifyFile("src//main//resources//metadata.csv", indexes);
		serializeTables(correctTable); // TODO
	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the rows to
	// update.
	// 2e3mel vector of pk values
	// check constraints
	// binary search
	// update
	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		boolean check = true;
		try {
			check = checkMinMaxForUpdate(strTableName, htblColNameValue);
		} catch (ClassNotFoundException e2) {
			e2.printStackTrace();
		} catch (IOException e2) {
			e2.printStackTrace();
		} catch (ParseException e2) {
			e2.printStackTrace();
		}
		Table correctTable = null;
		int pageIndex = 0;
		Object keyvalue = null;
		try {
			correctTable = deserializeTable(strTableName);
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		// To put input keys in vector
		Set<String> set = htblColNameValue.keySet();
		Iterator<String> iterator = set.iterator();
		Vector<Object> inputedKeys = new Vector<Object>();
		while (iterator.hasNext()) {
			inputedKeys.add(iterator.next());
		}

		// To put input values in vector
		Collection<Object> values = htblColNameValue.values();
		Vector<Object> inputValues = new Vector<Object>();
		for (Object value : values) {
			inputValues.add(value);
		}
		if (correctTable == null) {
			throw new DBAppException("Table Not found");
		}

		if (correctTable.getPageNames().isEmpty()) {
			throw new DBAppException("Empty Table");
		}
		if (check == false)
			throw new DBAppException("Check Min Max Values");
		else {
			if (correctTable.getPKType().equals("java.lang.Integer")) {
				keyvalue = Integer.parseInt(strClusteringKeyValue);
			} else if (correctTable.getPKType().equals("java.lang.String")) {
				keyvalue = strClusteringKeyValue;
			} else if (correctTable.getPKType().equals("java.lang.Double")) {
				keyvalue = Double.parseDouble(strClusteringKeyValue);
			} else if (correctTable.getPKType().equals("java.util.Date")) {
				try {
					keyvalue = new SimpleDateFormat("yyyy-MM-dd").parse(strClusteringKeyValue);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			if (!(correctTable.GridIndeces.isEmpty())) { // TODO
				System.out.println("Entered Grid Update");
				int pkRange = getPKRange(correctTable.getPrimaryKey(), keyvalue, correctTable);
				System.out.println(pkRange + " pkRange");
				for (GridIndex g : correctTable.GridIndeces) {
					if (g.getIndexes().contains(correctTable.getPrimaryKey()))
						System.out.println("Has PK");
					System.out.println(g.getBucketNames() + " Bucket Names");
					for (String s : g.getBucketNames()) {
						System.out.println(s + " ssssss");
						System.out.println(g.getIndexes().indexOf(correctTable.getPrimaryKey()) + "");
						System.out.println((s.charAt(g.getIndexes().indexOf(correctTable.getPrimaryKey())) + ""));
						if ((s.charAt(g.getIndexes().indexOf(correctTable.getPrimaryKey())) + "")
								.equals(pkRange + "")) {
							System.out.println("entered ifffffffffffff");
							Bucket b = null;
							try {
								b = deserializeBucket(correctTable, s, g.getName());
							} catch (ClassNotFoundException | IOException e1) {
								e1.printStackTrace();
							}
							int index = Collections.binarySearch(b.getPKs(), keyvalue);
							int bucketOverFlowIndex = searchBucketOverFlowIndex(correctTable, b, keyvalue);
							int rowIndex = index - (100 * (bucketOverFlowIndex));
							int[] reference = searchBucketOverFlow(b, correctTable, keyvalue, bucketOverFlowIndex,
									rowIndex);
							Page p = null;
							try {
								p = deserializePages(correctTable, reference[1]);
							} catch (ClassNotFoundException | IOException e) {
								e.printStackTrace();
							}

							Row r = p.getRow().get(reference[0]);
							for (int y = 0; y < inputedKeys.size(); y++) {
								if (inputedKeys.size() > correctTable.getColumnsName().size()) {
									throw new DBAppException("Column Not Found");
								} else {
									int hello = correctTable.getColumnsName().indexOf(inputedKeys.get(y));
									if (hello < 0)
										throw new DBAppException("Error");
									String test = correctTable.getColumnsType().get(hello);
									if (!(inputValues.get(y).getClass().getName().equals(test))) {
										throw new DBAppException("Cannot Insert: Data Type Not Applicable");
									} else {
										r.getValues().set(correctTable.getColumnsName().indexOf(inputedKeys.get(y)),
												inputValues.get(y));
									}
								}
							}

							serializePages(p, correctTable, pageIndex);
							serializeBuckets(b, correctTable, g.getName());
							serializeTables(correctTable);
						}
					}
				}
				// ////////////////////////////////////////

			} else {
				if (correctTable.getPKType().equals("java.lang.Integer")) {
					keyvalue = Integer.parseInt(strClusteringKeyValue);
				} else if (correctTable.getPKType().equals("java.lang.String")) {
					keyvalue = strClusteringKeyValue;
				} else if (correctTable.getPKType().equals("java.lang.Double")) {
					keyvalue = Double.parseDouble(strClusteringKeyValue);
				} else if (correctTable.getPKType().equals("java.util.Date")) {
					try {
						keyvalue = new SimpleDateFormat("yyyy-MM-dd").parse(strClusteringKeyValue);
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}

				int index = Collections.binarySearch(correctTable.getPKs(), keyvalue);

				if (!(index < 0)) {
					pageIndex = searchPageIndex(correctTable, keyvalue);
					Page p = null;
					try {
						p = deserializePages(correctTable, pageIndex);
					} catch (ClassNotFoundException | IOException e) {
						e.printStackTrace();
					}

					int rowIndex = index - (250 * (pageIndex));
					Row r = p.getRow().get(rowIndex);
					for (int y = 0; y < inputedKeys.size(); y++) {
						if (inputedKeys.size() > correctTable.getColumnsName().size()) {
							throw new DBAppException("Column Not Found");
						} else {
							int hello = correctTable.getColumnsName().indexOf(inputedKeys.get(y));
							if (hello < 0)
								throw new DBAppException("Error");
							String test = correctTable.getColumnsType().get(hello);
							if (!(inputValues.get(y).getClass().getName().equals(test))) {
								throw new DBAppException("Cannot Insert: Data Type Not Applicable");
							} else {
								r.getValues().set(correctTable.getColumnsName().indexOf(inputedKeys.get(y)),
										inputValues.get(y));
							}
						}
					}

					serializePages(p, correctTable, pageIndex);
					serializeTables(correctTable);
				} else {
					throw new DBAppException("There isnt a row with this key");
				}
			}
		}
	}

	public int getPKRange(String column, Object strClusteringKeyValue, Table t1) {
		int pkIndexRange = 0;

		if (strClusteringKeyValue.getClass().getSimpleName().equals("Integer")) {
			String min = t1.min.get(column);
			String max = t1.max.get(column);
			pkIndexRange = intRange(Integer.parseInt(min), Integer.parseInt(max), (int) strClusteringKeyValue);
		} else if (strClusteringKeyValue.getClass().getSimpleName().equals("String")) {
			if (column.equals("id")) {
				String min = t1.min.get(column);
				String max = t1.max.get(column);

				String[] parts = min.split("-");
				String minBefore = parts[0]; // 004
				String minAfter = parts[1];

				parts = max.split("-");
				String maxBefore = parts[0]; // 004
				String maxAfter = parts[1];

				parts = ((String) strClusteringKeyValue).split("-");
				String inputBefore = parts[0]; // 004
				String inputAfter = parts[1];

				pkIndexRange = idRange(Integer.parseInt(minBefore), Integer.parseInt(maxBefore),
						Integer.parseInt(minAfter), Integer.parseInt(maxAfter), Integer.parseInt(inputBefore),
						Integer.parseInt(inputAfter));
			} else {
				pkIndexRange = stringRange((String) strClusteringKeyValue);
			}
		} else if (strClusteringKeyValue.getClass().getSimpleName().equals("Double")) {
			String min = t1.min.get(column);
			String max = t1.max.get(column);
			pkIndexRange = doubleRange(Double.parseDouble(min), Double.parseDouble(max),
					(double) (strClusteringKeyValue));

		} else if (strClusteringKeyValue.getClass().getSimpleName().equals("Date")) {
			String minDate = t1.min.get(column);
			String maxDate = t1.max.get(column);

			String[] parts = minDate.split("-");
			String minYear = parts[0]; // 004
			String minMonth = parts[1];
			String minDay = parts[2];

			parts = maxDate.split("-");
			String maxYear = parts[0]; // 004
			String maxMonth = parts[1];
			String maxDay = parts[2];

			parts = ((String) strClusteringKeyValue).split("-");
			String inputYear = parts[0]; // 004
			String inputMonth = parts[1];
			String inputDay = parts[2];

			pkIndexRange = DateRange(Integer.parseInt(minYear), Integer.parseInt(maxYear), Integer.parseInt(minMonth),
					Integer.parseInt(maxMonth), Integer.parseInt(minDay), Integer.parseInt(maxDay),
					Integer.parseInt(inputYear), Integer.parseInt(inputMonth), Integer.parseInt(inputDay));
		}
		return pkIndexRange;
	}

	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue entries are ANDED together
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		Table correctTable = null;
		int pageIndex = 0;
		try {
			correctTable = deserializeTable(strTableName);
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		// To put input keys in vector
		Set<String> set = htblColNameValue.keySet();
		Iterator<String> iterator = set.iterator();
		Vector<String> inputedKeys = new Vector<String>();
		while (iterator.hasNext()) {
			inputedKeys.add(iterator.next());
		}

		// To put input values in vector
		Vector<Object> inputValues = new Vector<Object>();
		Collection<Object> values = htblColNameValue.values();
		for (Object value : values) {
			inputValues.add(value);
		}

		if (correctTable == null) {
			throw new DBAppException("Table Not found");
		}
		if (correctTable.getPageNames().isEmpty()) {
			throw new DBAppException("Empty Table");
		} else {
			System.out.println(correctTable.GridIndeces.size() + "how many grids");
			if (!(correctTable.GridIndeces.isEmpty())) {
				for (GridIndex g : correctTable.GridIndeces) {
					System.out.println(g.getName() + "grid nameeee");
					if (g.getIndexes().contains(correctTable.getPrimaryKey())
							&& inputedKeys.contains(correctTable.getPrimaryKey())) {
						System.out.println("Entered el ifayaa");
						// Binary Search
						Object keyvalue = inputValues.get(inputedKeys.indexOf(correctTable.PrimaryKey));
						int pkRange = getPKRange(correctTable.PrimaryKey, keyvalue, correctTable);
						System.out.println(keyvalue + "keyvalueeee");
						System.out.println(pkRange + "pkrangeeeee");
						System.out.println(g.getBucketNames() + " bucket Names");
						for (String s : g.getBucketNames()) {
							System.out.println(s + "bucketnameee loopppp");
							System.out.println(s.charAt(g.getIndexes().indexOf(correctTable.getPrimaryKey())) + "");
							System.out.println((s.charAt(g.getIndexes().indexOf(correctTable.getPrimaryKey())) + "")
									.equals(pkRange + "") + " checker for ifffff");
							if ((s.charAt(g.getIndexes().indexOf(correctTable.getPrimaryKey())) + "")
									.equals(pkRange + "")) {
								System.out.println("binary search");
								Bucket b = null;
								try {
									b = deserializeBucket(correctTable, s, g.getName());
								} catch (ClassNotFoundException | IOException e1) {
									e1.printStackTrace();
								}
								int index = Collections.binarySearch(b.getPKs(), keyvalue);
								System.out.println(index + "indexxxxx");
								int bucketOverFlowIndex = searchBucketOverFlowIndex(correctTable, b, keyvalue);
								System.out.println(bucketOverFlowIndex + "overflowwwindexxxxx");
								int rowIndex = index - (100 * (bucketOverFlowIndex));
								System.out.println(rowIndex + "rowwwwindexxxxx");
								int[] reference = searchBucketOverFlow(b, correctTable, keyvalue, bucketOverFlowIndex,
										rowIndex);
								for (int i : reference) {
									System.out.println(i + "referenceeeeeee");
								}

								Page p = null;
								try {
									p = deserializePages(correctTable, reference[1]);
								} catch (ClassNotFoundException | IOException e) {
									p = null;
								}
								if ((Collections.binarySearch(correctTable.getPKs(), keyvalue)) < 0) {
									if ((Collections.binarySearch(b.PKs, keyvalue)) < 0) {
										throw new DBAppException("There isnt a row with this key");
									} else {
										b.getBucketRow().remove(rowIndex);
										b.getPKs().remove(index);
									}
								} else {
									if (p != null) {
										boolean checker = true;
										System.out.println("ENTERED ELSEEEEEEEEE");
										Row r = p.getRow().get(reference[0]);
										Object pkindex = r.getValues()
												.get(correctTable.getColumnsName().indexOf(correctTable.PrimaryKey));
										p.getRow().remove(r);
										correctTable.getPKs().remove(pkindex);
										b.getBucketRow().remove(rowIndex);
										b.getPKs().remove(index);
										checker = true;
										if (p.getRow().isEmpty()) {
											serializePages(p, correctTable, reference[1]);
											File file = new File("src/main/resources/data/" + correctTable.getName()
													+ "Page" + reference[1]);
											if (file.delete()) {
												System.out.println("File deleted successfully");
											} else {
												System.out.println("Failed to delete the file");
											}
											correctTable.getPageNames().remove(reference[0]);
											for (int i = reference[1]; i < correctTable.getPageNames().size(); i++) {
												Page rename = null;
												try {
													rename = deserializePages(correctTable, i + 1);
												} catch (ClassNotFoundException | IOException e) {
													e.printStackTrace();
												}
												serializePages(rename, correctTable, reference[1]);
												serializeBuckets(b, correctTable, g.getName());
												serializeTables(correctTable);
											}
										} else {
											serializePages(p, correctTable, reference[1]);
											serializeBuckets(b, correctTable, g.getName());
											serializeTables(correctTable);
										}
									}

								}

							}
						}
					} else {
						System.out.println("entered else linear searchhhhhhh");
						// linear search
						String partialBucketIndex = partialIndex(inputedKeys, inputValues, correctTable, g);
						System.out.println(partialBucketIndex);
						System.out.println(g.getBucketNames() + " bucket Namesssss linearr");
						boolean bucketChecker = false;
						for (String s : g.getBucketNames()) {
							boolean check = false;
							for(int i = 0; i < partialBucketIndex.length(); i++) {
								
							}
							
							for (int i = 0; i < s.length() - 1; i++) {
								String substring = s.charAt(i) + "" + s.charAt(i + 1) + "";
								if (substring.equals(partialBucketIndex)) {
									check = true;
								}
							}
							System.out.println(check + "check linearrrr");
							if (check == true) {
								Bucket b = null;
								try {
									b = deserializeBucket(correctTable, s, g.getName());
								} catch (ClassNotFoundException | IOException e) {
									e.printStackTrace();
								} // linear search
								deleteBucketOverFlowLinear(b, correctTable, inputValues);
								bucketChecker = true;
								break;
							}
						}
						if (bucketChecker == false) {
							System.out.println("entered linear state");
							try {
								Iterator<String> pageNames = correctTable.getPageNames().iterator();
								while (pageNames.hasNext()) {
									String s = pageNames.next();
									Page p = deserializePages(correctTable, correctTable.getPageNames().indexOf(s));
									Iterator<Row> rows = p.getRow().iterator();
									while (rows.hasNext()) {
										boolean check = false;
										Row r = rows.next();
										for (int v = 0; v < inputValues.size(); v++) {
											if (r.getValues().contains(inputValues.get(v))) {
												check = true;
											} else {
												check = false;
												break;
											}
										}
										if (check == true) {
											Object keyvalue = r.getValues().get(correctTable.getPKindex());
											int index = Collections.binarySearch(correctTable.getPKs(), keyvalue);
											correctTable.getPKs().remove(keyvalue);
											rows.remove();
											if (p.getRow().isEmpty()) {
												serializePages(p, correctTable, pageIndex);
												File file = new File("src/main/resources/data/" + correctTable.getName()
														+ "Page" + pageIndex);
												if (file.delete()) {
													System.out.println("File deleted successfully");
												} else {
													System.out.println("Failed to delete the file");
												}
												pageNames.remove();
												for (int i = pageIndex; i < correctTable.getPageNames().size(); i++) {
													Page rename = deserializePages(correctTable, i + 1);
													serializePages(rename, correctTable, pageIndex);
												}
												break;
											} else {
												serializePages(p, correctTable, pageIndex);
											}
										}
									}
								}
							} catch (ClassNotFoundException | IOException e1) {
								e1.printStackTrace();
							}
						}
					}
				}
			} else {
				if (inputedKeys.contains(correctTable.getPrimaryKey())) {
					Object keyvalue = inputValues.get(inputedKeys.indexOf(correctTable.PrimaryKey));
					int index = Collections.binarySearch(correctTable.getPKs(), keyvalue);
					if (!(index < 0)) {
						pageIndex = searchPageIndex(correctTable, keyvalue);
						Page p = null;
						try {
							p = deserializePages(correctTable, pageIndex);
						} catch (ClassNotFoundException | IOException e) {
							e.printStackTrace();
						}
						int rowIndex = index - (250 * (pageIndex));
						Row r = p.getRow().get(rowIndex);
						p.getRow().remove(r);
						correctTable.getPKs().remove(index);
						if (p.getRow().isEmpty()) {
							serializePages(p, correctTable, pageIndex);
							File file = new File(
									"src/main/resources/data/" + correctTable.getName() + "Page" + pageIndex);
							if (file.delete()) {
								System.out.println("File deleted successfully");
							} else {
								System.out.println("Failed to delete the file");
							}
							correctTable.getPageNames().remove(pageIndex);
							for (int i = pageIndex; i < correctTable.getPageNames().size(); i++) {
								Page rename = null;
								try {
									rename = deserializePages(correctTable, i + 1);
								} catch (ClassNotFoundException | IOException e) {
									e.printStackTrace();
								}
								serializePages(rename, correctTable, pageIndex);
							}
						} else
							serializePages(p, correctTable, pageIndex);
					} else {
						throw new DBAppException("There isnt a row with this key");
					}

				}

				// If none of the input keys is a primary key use linear search and
				// delete all
				// rows that contain the input values
				else {
					System.out.println("entered linear state");
					try {
						Iterator<String> pageNames = correctTable.getPageNames().iterator();
						while (pageNames.hasNext()) {
							String s = pageNames.next(); // must be called before
															// you can call
															// i.remove()
							Page p = deserializePages(correctTable, correctTable.getPageNames().indexOf(s));
							Iterator<Row> rows = p.getRow().iterator();
							while (rows.hasNext()) {
								boolean check = false;
								Row r = rows.next();
								for (int v = 0; v < inputValues.size(); v++) {
									if (r.getValues().contains(inputValues.get(v))) {
										check = true;
									} else {
										check = false;
										break;
									}
								}
								if (check == true) {
									Object keyvalue = r.getValues().get(correctTable.getPKindex());
									int index = Collections.binarySearch(correctTable.getPKs(), keyvalue);
									correctTable.getPKs().remove(index);
									rows.remove();
									if (p.getRow().isEmpty()) {
										serializePages(p, correctTable, pageIndex);
										File file = new File("src/main/resources/data/" + correctTable.getName()
												+ "Page" + pageIndex);
										if (file.delete()) {
											System.out.println("File deleted successfully");
										} else {
											System.out.println("Failed to delete the file");
										}
										pageNames.remove();
										for (int i = pageIndex; i < correctTable.getPageNames().size(); i++) {
											Page rename = deserializePages(correctTable, i + 1);
											serializePages(rename, correctTable, pageIndex);
										}
										break;
									} else {
										serializePages(p, correctTable, pageIndex);
									}
								}
							}
						}
					} catch (ClassNotFoundException | IOException e1) {
						e1.printStackTrace();
					}

				}
				System.out.println("reached end");
				Vector v = new Vector();
				for (Object o : correctTable.getPKs()) {
					Page p = null;
					for (int i = 0; i < correctTable.getPageNames().size(); i++) {
						try {
							p = deserializePages(correctTable, i);
						} catch (ClassNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						Row r = null;
						for (int j = 0; j < p.getRow().size(); j++) {
							r = p.getRow().get(j);
							v.add(r.values.get(2));
						}
						serializePages(p, correctTable, i);
					}

				}
				for (int k = 0; k < correctTable.PKs.size(); k++) {
					if (v.contains(correctTable.PKs.get(k))) {
						continue;
					} else {
						correctTable.PKs.remove(k);
					}
				}
				serializeTables(correctTable);
			}
		}
	}

	// TODO
	private void deleteBucketOverFlowLinear(Bucket b, Table correctTable, Vector inputValues) {
		System.out.println("Entered recursions");
		int[] ref = new int[2];
		if (b == null) {
		} else {// TODO
			Iterator<Row> bucketRows = b.getBucketRow().iterator();
			while (bucketRows.hasNext()) {
				Row row = bucketRows.next();
				ref[0] = (int) row.values.get(row.values.size() - 2);
				ref[1] = (int) row.values.get(row.values.size() - 1);
				Page p = null;
				try {
					p = deserializePages(correctTable, ref[1]);
				} catch (ClassNotFoundException | IOException e) {
					e.printStackTrace();
				}
				Row r = p.getRow().get(ref[0]);
				boolean check = false;
				for (int v = 0; v < inputValues.size(); v++) {
					if (r.getValues().contains(inputValues.get(v))) {
						check = true;
					} else {
						check = false;
						break;
					}
				}

				if (check == true) {
					Object keyvalue = r.getValues().get(correctTable.getPKindex());
					int index = Collections.binarySearch(correctTable.getPKs(), keyvalue);
					correctTable.getPKs().remove(index);
					p.getRow().remove(r);
					if (p.getRow().isEmpty()) {
						serializePages(p, correctTable, ref[0]);
						File file = new File("src/main/resources/data/" + correctTable.getName() + "Page" + ref[0]);
						if (file.delete()) {
							System.out.println("File deleted successfully");
						} else {
							System.out.println("Failed to delete the file");
						}
						// TODO
						/*
						 * correctTable .getPageNames().remove(rows);
						 */
						for (int i = ref[1]; i < correctTable.getPageNames().size(); i++) {
							Page rename = null;
							try {
								rename = deserializePages(correctTable, i + 1);
							} catch (ClassNotFoundException | IOException e) {
								e.printStackTrace();
							}
							serializePages(rename, correctTable, ref[1]);
						}
						break;
					} else {
						/* serializePages(p, correctTable, pageIndex); */
					}
					bucketRows.remove();
					b.getPKs().remove(index);
				}

				//////////////////////////////////////

				/*
				 * Iterator<Row> rows = p.getRow().iterator(); while (rows.hasNext()) {
				 * 
				 * boolean check = false; Row r = rows.next(); for (int v = 0; v <
				 * inputValues.size(); v++) { if (r.getValues().contains(inputValues.get(v))) {
				 * check = true; } else { check = false; break; } }
				 * 
				 * if (check == true) { Object keyvalue =
				 * r.getValues().get(correctTable.getPKindex()); int index =
				 * Collections.binarySearch(correctTable.getPKs(), keyvalue);
				 * correctTable.getPKs().remove(index); rows.remove(); if (p.getRow().isEmpty())
				 * { serializePages(p, correctTable, ref[0]); File file = new
				 * File("src/main/resources/data/" + correctTable.getName() + "Page" + ref[0]);
				 * if (file.delete()) { System.out.println("File deleted successfully"); } else
				 * { System.out.println("Failed to delete the file"); } // TODO
				 * 
				 * correctTable .getPageNames().remove(rows);
				 * 
				 * for (int i = ref[1]; i < correctTable.getPageNames().size(); i++) { Page
				 * rename = null; try { rename = deserializePages(correctTable, i + 1); } catch
				 * (ClassNotFoundException | IOException e) { e.printStackTrace(); }
				 * serializePages(rename, correctTable, ref[1]); } break; } else {
				 * serializePages(p, correctTable, pageIndex); } } }
				 */

				////////////////////////////////
				/*
				 * Row rowInPage = p.getRow().get(ref[0]); p.getRow().remove(rowInPage); Object
				 * index =
				 * rowInPage.getValues().get(correctTable.columnsName.indexOf(correctTable.
				 * PrimaryKey)); correctTable.getPKs().remove(index); if (p.getRow().isEmpty())
				 * { serializePages(p, correctTable, ref[0]); File file = new
				 * File("src/main/resources/data/" + correctTable.getName() + "Page" + ref[1]);
				 * if (file.delete()) { System.out.println("File deleted successfully"); } else
				 * { System.out.println("Failed to delete the file"); }
				 * correctTable.getPageNames().remove(ref[1]); for (int i = ref[1]; i <
				 * correctTable.getPageNames().size(); i++) { Page rename = null; try { rename =
				 * deserializePages(correctTable, i + 1); } catch (ClassNotFoundException |
				 * IOException e) { e.printStackTrace(); } serializePages(rename, correctTable,
				 * ref[1]); } } else serializePages(p, correctTable, ref[1]);
				 */
			}
			deleteBucketOverFlowLinear(b.OverflowBucket, correctTable, inputValues);
		}
	}

	@SuppressWarnings("rawtypes")
	public List selectByPreference(SQLTerm[] array, String[] arrayOperators, Table correctTable) {
		Vector<SQLTerm> sql = new Vector<SQLTerm>();
		Vector<String> opr = new Vector<String>();
		Vector<Vector> rows = new Vector<Vector>();
		for (int i = 0; i < array.length; i++) {
			sql.add(array[i]);
		}
		for (int i = 0; i < arrayOperators.length; i++) {
			opr.add(arrayOperators[i]);
		}
		for (int i = 0; i < sql.size(); i++) {
			rows.add(insideOper(sql.get(i), correctTable));
		}
		int i = 0;
		while (rows.size() != 1 && i < opr.size() - 1) {
			if (opr.get(0).equals("OR")) {
				Vector<Row> r = ORoperator(rows.get(0), rows.get(1));
				opr.remove(0);
				rows.remove(0);
				rows.remove(1);
				rows.insertElementAt(r, 0);
			} else if (opr.get(0).equals("AND")) {
				Vector<Row> r = ANDoperator(rows.get(0), rows.get(1), sql.get(i), sql.get(i + 1));
				opr.remove(0);
				rows.remove(0);
				rows.remove(1);
				rows.insertElementAt(r, 0);
			} else if (opr.get(0).equals("XOR")) {
				Vector<Row> r = XORoperator(rows.get(0), rows.get(1), sql.get(i), sql.get(i + 1));
				opr.remove(0);
				rows.remove(0);
				rows.remove(1);
				rows.insertElementAt(r, 0);
			}
			i++;
		}
		return rows;
	}

	public Vector<Row> ORoperator(Vector<Row> op1, Vector<Row> op2) {
		Vector<Row> res = new Vector<Row>();
		for (int i = 0; i < op1.size(); i++) {
			res.add(op1.get(i));
		}
		for (int i = 0; i < op2.size(); i++) {
			res.add(op2.get(i));
		}
		return res;
	}

	public Vector<Row> ANDoperator(Vector<Row> op1, Vector<Row> op2, SQLTerm sql1, SQLTerm sql2) {
		Vector<Row> res = new Vector<Row>();
		for (int i = 0; i < op1.size(); i++) {
			if (op1.get(i).values.contains(sql1._objValue) && op1.get(i).values.contains(sql2._objValue)) {
				res.add(op1.get(i));
			}
		}
		for (int i = 0; i < op2.size(); i++) {
			if (op2.get(i).values.contains(sql1._objValue) && op2.get(i).values.contains(sql2._objValue)) {
				res.add(op2.get(i));
			}
		}
		return res;
	}

	public Vector<Row> XORoperator(Vector<Row> op1, Vector<Row> op2, SQLTerm sql1, SQLTerm sql2) {
		Vector<Row> res = new Vector<Row>();
		for (int i = 0; i < op1.size(); i++) {
			if (op1.get(i).values.contains(sql1._objValue) && !op1.get(i).values.contains(sql2._objValue)) {
				res.add(op1.get(i));
			}
		}
		for (int i = 0; i < op2.size(); i++) {
			if (op2.get(i).values.contains(sql1._objValue) && !op2.get(i).values.contains(sql2._objValue)) {
				res.add(op2.get(i));
			}
		}
		return res;
	}

	public Vector<Row> insideOper(SQLTerm sqlTerm, Table correctTable) {
		Vector<Row> list = new Vector<Row>();
		for (int j = 0; j < correctTable.PageNames.size(); j++) {
			Page pageToUse = null;
			try {
				pageToUse = deserializePages(correctTable, j);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			Row row = null;
			for (int k = 0; k < pageToUse.Rows.size(); k++) {
				row = pageToUse.getRow().get(k);
				switch (sqlTerm._strOperator) {
				case ">":
					if (row.getValues().get(counter).toString().compareTo(sqlTerm._objValue.toString()) > 0) {
						list.add(row);
					}
					break;
				case ">=":
					if (row.getValues().get(counter).toString().compareTo(sqlTerm._objValue.toString()) >= 0) {
						list.add(row);
					}
					break;
				case "<":
					if (row.getValues().get(counter).toString().compareTo(sqlTerm._objValue.toString()) < 0) {
						list.add(row);
					}
					break;
				case "=<":
					if (row.getValues().get(counter).toString().compareTo(sqlTerm._objValue.toString()) <= 0) {
						list.add(row);
					}
					break;
				case "!=":
					if (row.getValues().get(counter).toString().equals(sqlTerm._objValue.toString())) {
						list.add(row);
					}
					break;
				case "=":
					if (row.getValues().get(counter).toString().equals(sqlTerm._objValue.toString())) {
						list.add(row);
					}
					break;
				}
			}
			serializePages(pageToUse, correctTable, j);
		}
		return list;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		List list = new Vector<>();
		Vector<String> strColumnNames = new Vector<String>();
		Vector<String> strOperators = new Vector<String>();
		Vector<Object> objValues = new Vector<Object>();

		for (SQLTerm s : arrSQLTerms) {
			strColumnNames.add(s._strColumnName);
			strOperators.add(s._strOperator);
			objValues.add(s._objValue);
		}

		Table correctTable = null;
		try {
			correctTable = deserializeTable(arrSQLTerms[0]._strTableName);
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		int pageIndex = 0;
		boolean pkChecker = false;
		SQLTerm[] array = arrSQLTerms;
		String[] array_two = strarrOperators;
		for (int i = 0; i < array.length - 1; i++) {
			if (array[i]._strTableName != array[i + 1]._strTableName) {
				throw new DBAppException("Inner Join Not Supported");
			} else
				continue;
		}
		if (arrSQLTerms.length == 0) {
			return null;
		}
		if (strarrOperators.length + 1 != arrSQLTerms.length) {
			throw new DBAppException("Invalid input");
		}
		if (!correctTable.GridIndeces.isEmpty()) {
			boolean gridchecker = false;
			for (GridIndex grid : correctTable.GridIndeces) {
				System.out.println(grid.getName() + " nameee");
				boolean checker = false;
				System.out.println(strColumnNames);
				for (String s : strColumnNames) {
					if (grid.getIndexes().contains(s)) {
						checker = true;
					} else {
						checker = false;
						break;
					}
				}
				System.out.println(checker + " omar galal");
				if (checker == true) {
					Vector<String> sortedColNames = new Vector<String>();
					Vector<Object> sortedObjValues = new Vector<Object>();
					for (String t : grid.getIndexes()) {
						if (strColumnNames.contains(t)) {
							sortedColNames.add(t);
							sortedObjValues.add(objValues.get(strColumnNames.indexOf(t)));
						}
					}

					String partialBucketIndex = partialIndex(sortedColNames, sortedObjValues, correctTable, grid);
					System.out.println(partialBucketIndex + " partial bucket index");
					for (String s : grid.getBucketNames()) {
						System.out.println(s);
						boolean check = false;
						for (int i = 0; i < s.length() - 1; i++) {
							String substring = s.charAt(i) + "" + s.charAt(i + 1) + "";
							if (substring.equals(partialBucketIndex)) {
								check = true;
							}
						}
						if (check == true) {
							Bucket b = null;
							try {
								b = deserializeBucket(correctTable, s, grid.getName());
							} catch (ClassNotFoundException | IOException e) {
								e.printStackTrace();
							}
							// If one of index pk use binary
							if (strColumnNames.contains(correctTable.PrimaryKey)) {
								Object keyvalue = objValues.get(strColumnNames.indexOf(correctTable.PrimaryKey));
								System.out.println(keyvalue + " el arb3a");
								System.out.println(b.getPKs() + "bpkk");
								int index = Collections.binarySearch(b.getPKs(), keyvalue);
								System.out.println(index);
								int bucketOverFlowIndex = searchBucketOverFlowIndex(correctTable, b, keyvalue);
								System.out.println(bucketOverFlowIndex);
								int rowIndex = index - (100 * (bucketOverFlowIndex));
								System.out.println(rowIndex);
								int[] reference = searchBucketOverFlow(b, correctTable, keyvalue, bucketOverFlowIndex,
										rowIndex);
								for (int x : reference) {
									System.out.println(x + " xxx");
								}
								Page p = null;
								try {
									p = deserializePages(correctTable, reference[1]);
								} catch (ClassNotFoundException | IOException e) {
									e.printStackTrace();
								}

								Row r = p.getRow().get(reference[0]);
								list.add(r);
							}
							// linear search
							else {
								List n = new Vector();
								list = searchBucketOverFlowLinear(b, correctTable, sortedObjValues, n);
							}
							gridchecker = true;
						}
					}
				}
				if (gridchecker == true) {
					break;
				}
			}
			if (gridchecker == false) {
				System.out.println("entered if false");
				if (strarrOperators.length >= 2) {
					list = selectByPreference(array, strarrOperators, correctTable);
				} else {
					for (SQLTerm s : arrSQLTerms) {
						try {
							if (deserializeTable(s._strTableName) == null)
								throw new DBAppException("Table not found");
							if (!(deserializeTable(s._strTableName).getColumnsName().contains(s._strColumnName)))
								throw new DBAppException("Column not found");
							if (s._strColumnName.equals(deserializeTable(s._strTableName).getPrimaryKey())) {
								pkChecker = true;
							}
							strColumnNames.add(s._strColumnName);
							strOperators.add(s._strOperator);
							objValues.add(s._objValue);
						} catch (ClassNotFoundException | IOException e) {
							e.printStackTrace();
						}
						for (int p = 0; p < strarrOperators.length; p++) {
							switch (strarrOperators[p]) {
							case "OR":
								System.out.println("OR case");
								int counter = 0;
								for (int i = 0; i < array.length; i++) {
									System.out.println("2nd for");
									counter = correctTable.columnsName.indexOf(array[i]._strColumnName);
									System.out.println(correctTable.PageNames.size() + " test");
									for (int j = 0; j < correctTable.PageNames.size(); j++) {
										System.out.println("3rd for");
										Page pageToUse = null;
										try {
											pageToUse = deserializePages(correctTable, j);
										} catch (ClassNotFoundException e) {
											e.printStackTrace();
										} catch (IOException e) {
											e.printStackTrace();
										}
										Row row = null;
										for (int k = 0; k < pageToUse.Rows.size(); k++) {
											row = pageToUse.getRow().get(k);
											System.out.println(row.values);
											switch (array[i]._strOperator) {
											case ">":
												if (row.getValues().get(counter).toString()
														.compareTo(array[i]._objValue.toString()) > 0) {
													list.add(row);
												}
												break;
											case ">=":
												if (row.getValues().get(counter).toString()
														.compareTo(array[i]._objValue.toString()) >= 0) {
													list.add(row);
												}
												break;
											case "<":
												if (row.getValues().get(counter).toString()
														.compareTo(array[i]._objValue.toString()) < 0) {
													list.add(row);
												}
												break;
											case "=<":
												if (row.getValues().get(counter).toString()
														.compareTo(array[i]._objValue.toString()) <= 0) {
													list.add(row);
												}
												break;
											case "!=":
												if (row.getValues().get(counter).toString()
														.equals(array[i]._objValue.toString())) {
													list.add(row);
												}
												break;
											case "=":
												if (row.getValues().get(counter).toString()
														.equals(array[i]._objValue.toString())) {
													list.add(row);
												}
												break;
											}
										}
										serializePages(pageToUse, correctTable, j);
									}
								}
								break;
							case "AND":
								int counter1 = 0;
								Vector<Integer> counters = new Vector<Integer>();
								for (int i = 0; i < array.length; i++) {
									counter1 = correctTable.columnsName.indexOf(array[i]._strColumnName);
									counters.add(counter1);
									for (int j = 0; j < correctTable.PageNames.size(); j++) {
										Page pageToUse = null;
										try {
											pageToUse = deserializePages(correctTable, j);
										} catch (ClassNotFoundException e) {
											e.printStackTrace();
										} catch (IOException e) {
											e.printStackTrace();
										}
										Row row = null;
										for (int k = 0; k < pageToUse.Rows.size(); k++) {
											row = pageToUse.getRow().get(k);
											switch (array[i]._strOperator) {
											case ">":
												if (row.getValues().get(counter1).toString()
														.compareTo(array[i]._objValue.toString()) > 0) {
													list.add(row);
												}
												break;
											case ">=":
												if (row.getValues().get(counter1).toString()
														.compareTo(array[i]._objValue.toString()) >= 0) {
													list.add(row);
												}
												break;
											case "<":
												if (row.getValues().get(counter1).toString()
														.compareTo(array[i]._objValue.toString()) < 0) {
													list.add(row);
												}
												break;
											case "=<":
												if (row.getValues().get(counter1).toString()
														.compareTo(array[i]._objValue.toString()) <= 0) {
													list.add(row);
												}
												break;
											case "!=":
												if (row.getValues().get(counter1).toString()
														.equals(array[i]._objValue.toString())) {
													list.add(row);
												}
												break;
											case "=":
												if (row.getValues().get(counter1).toString()
														.equals(array[i]._objValue.toString())) {
													list.add(row);
												}
												break;
											}
										}
										serializePages(pageToUse, correctTable, j);
									}
								}
								boolean isTrue = false;
								List newList = new Vector<>();
								for (int i = 0, j = 0; j < list.size() && i < array.length - 1; j++, i++) {
									Row rowTemp = (Row) list.get(j);
									if (array.length == 2) {
										if (rowTemp.values.contains(array[i]._objValue)
												&& rowTemp.values.contains(array[i + 1]._objValue)) {
											newList.add(rowTemp);
										}
									}
									list = newList;
								}
								break;
							case "XOR":
								int counter11 = 0;
								for (int i = 0; i < array.length; i++) {
									counter11 = correctTable.columnsName.indexOf(array[i]._strColumnName);
									for (int j = 0; j < correctTable.PageNames.size(); j++) {
										Page pageToUse = null;
										try {
											pageToUse = deserializePages(correctTable, j);
										} catch (ClassNotFoundException e) {
											e.printStackTrace();
										} catch (IOException e) {
											e.printStackTrace();
										}
										Row row = null;
										for (int k = 0; k < pageToUse.Rows.size(); k++) {
											row = pageToUse.getRow().get(k);
											switch (array[i]._strOperator) {
											case ">":
												if (row.getValues().get(counter11).toString()
														.compareTo(array[i]._objValue.toString()) > 0) {
													list.add(row);
												}
												break;
											case ">=":
												if (row.getValues().get(counter11).toString()
														.compareTo(array[i]._objValue.toString()) >= 0) {
													list.add(row);
												}
												break;
											case "<":
												if (row.getValues().get(counter11).toString()
														.compareTo(array[i]._objValue.toString()) < 0) {
													list.add(row);
												}
												break;
											case "=<":
												if (row.getValues().get(counter11).toString()
														.compareTo(array[i]._objValue.toString()) <= 0) {
													list.add(row);
												}
												break;
											case "!=":
												if (row.getValues().get(counter11).toString()
														.equals(array[i]._objValue.toString())) {
													list.add(row);
												}
												break;
											case "=":
												if (row.getValues().get(counter11).toString()
														.equals(array[i]._objValue.toString())) {
													list.add(row);
												}
												break;
											}
										}
										serializePages(pageToUse, correctTable, j);
									}
								}
								List newList1 = new Vector<>();
								for (int i = 0, j = 0; j < list.size() && i < array.length - 1; j++, i++) {
									Row rowTemp = (Row) list.get(j);
									if (array.length == 2) {
										if (rowTemp.values.contains(array[i]._objValue)
												&& !rowTemp.values.contains(array[i + 1]._objValue)) {
											newList1.add(rowTemp);
										}
										if (!rowTemp.values.contains(array[i]._objValue)
												&& rowTemp.values.contains(array[i + 1]._objValue)) {
											newList1.add(rowTemp);
										}
									}
									list = newList1;
								}
								break;
							default:
								throw new DBAppException("InValid Input");
							}
						}
					}
				}
			}
		} else {
			if (strarrOperators.length >= 2) {
				list = selectByPreference(array, strarrOperators, correctTable);
			} else {
				for (SQLTerm s : arrSQLTerms) {
					try {
						if (deserializeTable(s._strTableName) == null)
							throw new DBAppException("Table not found");
						if (!(deserializeTable(s._strTableName).getColumnsName().contains(s._strColumnName)))
							throw new DBAppException("Column not found");
						if (s._strColumnName.equals(deserializeTable(s._strTableName).getPrimaryKey())) {
							pkChecker = true;
						}
						strColumnNames.add(s._strColumnName);
						strOperators.add(s._strOperator);
						objValues.add(s._objValue);
					} catch (ClassNotFoundException | IOException e) {
						e.printStackTrace();
					}
					for (int p = 0; p < strarrOperators.length; p++) {
						switch (strarrOperators[p]) {
						case "OR":
							int counter = 0;
							for (int i = 0; i < array.length; i++) {
								counter = correctTable.columnsName.indexOf(array[i]._strColumnName);
								for (int j = 0; j < correctTable.PageNames.size(); j++) {
									Page pageToUse = null;
									try {
										pageToUse = deserializePages(correctTable, j);
									} catch (ClassNotFoundException e) {
										e.printStackTrace();
									} catch (IOException e) {
										e.printStackTrace();
									}
									Row row = null;
									for (int k = 0; k < pageToUse.Rows.size(); k++) {
										row = pageToUse.getRow().get(k);
										switch (array[i]._strOperator) {
										case ">":
											if (row.getValues().get(counter).toString()
													.compareTo(array[i]._objValue.toString()) > 0) {
												list.add(row);
											}
											break;
										case ">=":
											if (row.getValues().get(counter).toString()
													.compareTo(array[i]._objValue.toString()) >= 0) {
												list.add(row);
											}
											break;
										case "<":
											if (row.getValues().get(counter).toString()
													.compareTo(array[i]._objValue.toString()) < 0) {
												list.add(row);
											}
											break;
										case "=<":
											if (row.getValues().get(counter).toString()
													.compareTo(array[i]._objValue.toString()) <= 0) {
												list.add(row);
											}
											break;
										case "!=":
											if (row.getValues().get(counter).toString()
													.equals(array[i]._objValue.toString())) {
												list.add(row);
											}
											break;
										case "=":
											if (row.getValues().get(counter).toString()
													.equals(array[i]._objValue.toString())) {
												list.add(row);
											}
											break;
										}
									}
									serializePages(pageToUse, correctTable, j);
								}
							}
							break;
						case "AND":
							int counter1 = 0;
							Vector<Integer> counters = new Vector<Integer>();
							for (int i = 0; i < array.length; i++) {
								counter1 = correctTable.columnsName.indexOf(array[i]._strColumnName);
								counters.add(counter1);
								for (int j = 0; j < correctTable.PageNames.size(); j++) {
									Page pageToUse = null;
									try {
										pageToUse = deserializePages(correctTable, j);
									} catch (ClassNotFoundException e) {
										e.printStackTrace();
									} catch (IOException e) {
										e.printStackTrace();
									}
									Row row = null;
									for (int k = 0; k < pageToUse.Rows.size(); k++) {
										row = pageToUse.getRow().get(k);
										switch (array[i]._strOperator) {
										case ">":
											if (row.getValues().get(counter1).toString()
													.compareTo(array[i]._objValue.toString()) > 0) {
												list.add(row);
											}
											break;
										case ">=":
											if (row.getValues().get(counter1).toString()
													.compareTo(array[i]._objValue.toString()) >= 0) {
												list.add(row);
											}
											break;
										case "<":
											if (row.getValues().get(counter1).toString()
													.compareTo(array[i]._objValue.toString()) < 0) {
												list.add(row);
											}
											break;
										case "=<":
											if (row.getValues().get(counter1).toString()
													.compareTo(array[i]._objValue.toString()) <= 0) {
												list.add(row);
											}
											break;
										case "!=":
											if (row.getValues().get(counter1).toString()
													.equals(array[i]._objValue.toString())) {
												list.add(row);
											}
											break;
										case "=":
											if (row.getValues().get(counter1).toString()
													.equals(array[i]._objValue.toString())) {
												list.add(row);
											}
											break;
										}
									}
									serializePages(pageToUse, correctTable, j);
								}
							}
							boolean isTrue = false;
							List newList = new Vector<>();
							for (int i = 0, j = 0; j < list.size() && i < array.length - 1; j++, i++) {
								Row rowTemp = (Row) list.get(j);
								if (array.length == 2) {
									if (rowTemp.values.contains(array[i]._objValue)
											&& rowTemp.values.contains(array[i + 1]._objValue)) {
										newList.add(rowTemp);
									}
								}
								list = newList;
							}
							break;
						case "XOR":
							int counter11 = 0;
							for (int i = 0; i < array.length; i++) {
								counter11 = correctTable.columnsName.indexOf(array[i]._strColumnName);
								for (int j = 0; j < correctTable.PageNames.size(); j++) {
									Page pageToUse = null;
									try {
										pageToUse = deserializePages(correctTable, j);
									} catch (ClassNotFoundException e) {
										e.printStackTrace();
									} catch (IOException e) {
										e.printStackTrace();
									}
									Row row = null;
									for (int k = 0; k < pageToUse.Rows.size(); k++) {
										row = pageToUse.getRow().get(k);
										switch (array[i]._strOperator) {
										case ">":
											if (row.getValues().get(counter11).toString()
													.compareTo(array[i]._objValue.toString()) > 0) {
												list.add(row);
											}
											break;
										case ">=":
											if (row.getValues().get(counter11).toString()
													.compareTo(array[i]._objValue.toString()) >= 0) {
												list.add(row);
											}
											break;
										case "<":
											if (row.getValues().get(counter11).toString()
													.compareTo(array[i]._objValue.toString()) < 0) {
												list.add(row);
											}
											break;
										case "=<":
											if (row.getValues().get(counter11).toString()
													.compareTo(array[i]._objValue.toString()) <= 0) {
												list.add(row);
											}
											break;
										case "!=":
											if (row.getValues().get(counter11).toString()
													.equals(array[i]._objValue.toString())) {
												list.add(row);
											}
											break;
										case "=":
											if (row.getValues().get(counter11).toString()
													.equals(array[i]._objValue.toString())) {
												list.add(row);
											}
											break;
										}
									}
									serializePages(pageToUse, correctTable, j);
								}
							}
							List newList1 = new Vector<>();
							for (int i = 0, j = 0; j < list.size() && i < array.length - 1; j++, i++) {
								Row rowTemp = (Row) list.get(j);
								if (array.length == 2) {
									if (rowTemp.values.contains(array[i]._objValue)
											&& !rowTemp.values.contains(array[i + 1]._objValue)) {
										newList1.add(rowTemp);
									}
									if (!rowTemp.values.contains(array[i]._objValue)
											&& rowTemp.values.contains(array[i + 1]._objValue)) {
										newList1.add(rowTemp);
									}
								}
								list = newList1;
							}
							break;
						default:
							throw new DBAppException("InValid Input");
						}
					}
				}
			}
		}
		serializeTables(correctTable);
		Iterator iterator = list.iterator();
		System.out.println("iter");
		return iterator;
	}

	// //////////////////////////Methods/////////////////////////////////////////////

	private String partialIndex(Vector<String> sortedColNames, Vector<Object> sortedObjValues, Table t1,
			GridIndex gridToBeUsed) {
		System.out.println("method enteredddddddddddd");
		System.out.println(sortedColNames);
		System.out.println(sortedObjValues);
		String BucketIndex = "";
		for (String s : sortedColNames) {
			Object value = sortedObjValues.get(sortedColNames.indexOf(s));
			System.out.println(value + "Value");
			System.out.println(value.getClass() + " class");
			System.out.println(value.getClass().getSimpleName().equals("String"));
			if (value.getClass().getSimpleName().equals("Integer")) {
				String min = t1.min.get(s);
				String max = t1.max.get(s);
				int i = intRange(Integer.parseInt(min), Integer.parseInt(max), (int) value);
				BucketIndex += i;
				System.out.println(BucketIndex + "BucketIndex------------");
			} else if (value.getClass().getSimpleName().equals("String")) {
				if (s.equals("id")) {
					String min = t1.min.get(s);
					String max = t1.max.get(s);

					String[] parts = min.split("-");
					String minBefore = parts[0]; // 004
					String minAfter = parts[1];

					parts = max.split("-");
					String maxBefore = parts[0]; // 004
					String maxAfter = parts[1];

					parts = ((String) value).split("-");
					String inputBefore = parts[0]; // 004
					String inputAfter = parts[1];

					int i = idRange(Integer.parseInt(minBefore), Integer.parseInt(maxBefore),
							Integer.parseInt(minAfter), Integer.parseInt(maxAfter), Integer.parseInt(inputBefore),
							Integer.parseInt(inputAfter));
					BucketIndex += i;
					System.out.println(BucketIndex + "BcketIndex------------");
				} else {
					int i = stringRange((String) value);
					BucketIndex += i;
					System.out.println(BucketIndex + "BucketIndex------------");
				}
			} else if (value.getClass().getSimpleName().equals("Double")) {
				String min = t1.min.get(s);
				String max = t1.max.get(s);
				int i = doubleRange(Double.parseDouble(min), Double.parseDouble(max), (double) value);
				BucketIndex += i;
				System.out.println(BucketIndex + "BucketIndex------------");

			} else if (value.getClass().getSimpleName().equals("Date")) {
				String minDate = t1.min.get(s);
				String maxDate = t1.max.get(s);

				String[] parts = minDate.split("-");
				String minYear = parts[0]; // 004
				String minMonth = parts[1];
				String minDay = parts[2];

				parts = maxDate.split("-");
				String maxYear = parts[0]; // 004
				String maxMonth = parts[1];
				String maxDay = parts[2];

				parts = ((String) value).split("-");
				String inputYear = parts[0]; // 004
				String inputMonth = parts[1];
				String inputDay = parts[2];

				int i = DateRange(Integer.parseInt(minYear), Integer.parseInt(maxYear), Integer.parseInt(minMonth),
						Integer.parseInt(maxMonth), Integer.parseInt(minDay), Integer.parseInt(maxDay),
						Integer.parseInt(inputYear), Integer.parseInt(inputMonth), Integer.parseInt(inputDay));
				BucketIndex += i;
				System.out.println(BucketIndex + "BucketIndex------------");
			}
		}
		System.out.println(BucketIndex + "a7eehhhhh");
		return BucketIndex;
	}

	public static List searchBucketOverFlowLinear(Bucket b, Table t, Vector<Object> insertedValues, List list) {
		int[] ref = new int[2];
		if (b == null) {
		} else {
			for (Row r : b.getBucketRow()) {
				ref[0] = (int) r.values.get(r.values.size() - 2);
				ref[1] = (int) r.values.get(r.values.size() - 1);
				Page p = null;
				try {
					p = deserializePages(t, ref[0]);
				} catch (ClassNotFoundException | IOException e) {
					e.printStackTrace();
				}
				Row rowInPage = p.getRow().get(ref[1]);
				boolean checker = false;
				for (Object o : insertedValues) {
					if (rowInPage.values.contains(o)) {
						checker = true;
					} else {
						checker = false;
						break;
					}
				}
				if (checker = true) {
					list.add(rowInPage);
				}
				serializePages(p, t, ref[0]);
			}
			searchBucketOverFlowLinear(b.OverflowBucket, t, insertedValues, list);
		}
		return list;
	}

	public static int[] searchBucketOverFlow(Bucket b, Table t, Object primaryKeyValue, int overflowCounter,
			int referenceIndex) {
		System.out.println("methodddd");
		int[] temp = new int[2];
		if (overflowCounter == 0) {
			System.out.println(b.getBucketRow().get(referenceIndex).values);
			temp[0] = (int) b.getBucketRow().get(referenceIndex).values
					.get(b.getBucketRow().get(referenceIndex).values.size() - 2);
			temp[1] = (int) b.getBucketRow().get(referenceIndex).values
					.get(b.getBucketRow().get(referenceIndex).values.size() - 1);
		} else {
			return searchBucketOverFlow(b.OverflowBucket, t, primaryKeyValue, overflowCounter--, referenceIndex);
		}
		return temp;
	}

	public static Object AssignMin(Page page, Table t) {
		Object min = page.getRow().get(0).getValues().get(t.getPKindex());
		return min;
	}

	public static Object AssignMax(Page page, Table t) {
		Object max = page.getRow().get(page.getRow().size() - 1).getValues().get(t.getPKindex());
		return max;
	}

	public static void serializeBuckets(Bucket bucket, Table t1, String GridName) {
		try {
			FileOutputStream fileOut = new FileOutputStream(
					"src/main/resources/data/" + t1.getName() + " " + GridName + " Bucket " + bucket.getBucketName());
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(bucket);
			out.close();
			fileOut.close();
		} catch (Exception i1) {
			i1.printStackTrace();
		}
	}

	private static Bucket deserializeBucket(Table t, String index, String GridName)
			throws IOException, ClassNotFoundException {
		Bucket p = null;
		FileInputStream fis = new FileInputStream(
				new File("src/main/resources/data/" + t.getName() + " " + GridName + " Bucket " + index));
		ObjectInputStream in = new ObjectInputStream(fis);
		p = (Bucket) in.readObject();
		in.close();
		fis.close();
		return p;
	}

	/*
	 * public static <T> Vector<Vector<T>> cartesianProduct(List<Vector<T>> list,
	 * int size, Table t1, String GridName) { Vector<Vector<T>> resultLists = new
	 * Vector<Vector<T>>(); int i = 0; if (list.size() == 0) { resultLists.add(new
	 * Vector<T>()); return resultLists; } else { Vector<T> firstList = list.get(0);
	 * Vector<Vector<T>> remainingLists = cartesianProduct(list.subList(1,
	 * list.size()), size, t1, GridName); for (T condition : firstList) { for
	 * (Vector<T> remainingList : remainingLists) { Vector<T> resultList = new
	 * Vector<T>(); resultList.add(condition); resultList.addAll(remainingList); if
	 * (resultList.size() == size) { String index = ""; for (int k = 0; k <
	 * resultList.size(); k++) { index += list.get(k).indexOf(resultList.get(k)); }
	 * Bucket bucket = new Bucket(); bucket.setBucketName(" " + index);
	 * System.out.println(resultList + " and" + i++); serializeBuckets(bucket, t1,
	 * GridName); }
	 * 
	 * resultLists.add(resultList); } } } return resultLists; }
	 */

	@SuppressWarnings("null")
	public static int idRange(int minBefore, int maxBefore, int minAfter, int maxAfter, int inputBefore,
			int inputAfter) {

		double total_length = (maxBefore - minBefore);
		double subrange_length = (total_length / 10);

		double current_start = minBefore;

		for (int i = 0; i < 10; ++i) {
			if (i == 9) {
				current_start -= 9;
			}
			System.out.println("Smaller range: [" + (int) current_start + "-" + String.format("%04d", (int) minAfter)
					+ ", " + ((int) (current_start + subrange_length)) + "-" + String.format("%04d", (int) (maxAfter))
					+ "]");
			if ((Integer.compare(inputBefore, (int) current_start) > 0
					|| Integer.compare(inputBefore, (int) current_start) == 0)
					&& (Integer.compare(inputBefore, (int) (current_start + subrange_length)) < 0)
					|| Integer.compare(inputBefore, (int) (current_start + subrange_length)) == 0) {
				return i;
			} else {
				System.out.println("Not in Range");
			}
			current_start += subrange_length + 1;
		}
		return (Integer) null;
	}

	@SuppressWarnings("null")
	public static int stringRange(String string) {
		int x1 = 'A';
		int y1 = 'Z';

		double total_length1 = y1 - x1;
		double subrange_length1 = total_length1 / 5;

		String min = "AAAAAA";

		for (int i = 0; i < 5; ++i) {
			String t = "";
			for (int j = 0; j < min.length(); j++) {
				if (i == 4) {
					int x11 = min.charAt(j);
					double l = x11 + subrange_length1 - 4;

					t += (char) (l);
				} else {
					int x11 = min.charAt(j);
					double l = x11 + subrange_length1;

					t += (char) (l);
				}

			}

			System.out.println("Smaller range: [" + min + ", " + t + "]");
			if (string.compareTo(min) > 0 && string.compareTo(t) < 0) {
				return i;
			} else {
				System.out.println("Not in Range");
			}
			String m = "";
			for (int j = 0; j < t.length(); j++) {
				int x3 = t.charAt(j);
				m += (char) (x3 + 1);
			}
			min = m;
		}

		// System.out.println("Lower");
		int x = 'a';
		int y = 'z';

		double total_length = y - x;
		double subrange_length = total_length / 5;

		String min2 = "aaaaa";

		for (int i = 0; i < 5; ++i) {
			String t = "";
			for (int j = 0; j < min2.length(); j++) {
				if (i == 4) {
					int x11 = min2.charAt(j);
					double l = x11 + subrange_length - 4;

					t += (char) (l);
				} else {
					int x11 = min2.charAt(j);
					double l = x11 + subrange_length;

					t += (char) (l);
				}

			}

			System.out.println("Smaller range: [" + min2 + ", " + t + "]");
			if (string.compareTo(min2) > 0 && string.compareTo(t) < 0) {
				return (i + 5);
			} else {
				System.out.println("Not in Range");
			}
			String m = "";
			for (int j = 0; j < t.length(); j++) {
				int x3 = t.charAt(j);
				m += (char) (x3 + 1);
			}
			min2 = m;
		}
		return (Integer) null;
	}

	public static int intRange(int min, int max, int input) {
		int total_length = max - min;
		int subrange_length = total_length / 10;

		int current_start = min;
		int i = 0;
		for (i = 0; i < 10; ++i) {
			System.out.println("Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
			if (Integer.compare(input, current_start) > 0
					&& Integer.compare(input, (int) (current_start + subrange_length)) < 0) {
				System.out.println("In Range " + i + "-------");
				return i;
			} else {
				System.out.println("Not in Range");
			}
			current_start += subrange_length;
		}
		return i;
	}

	@SuppressWarnings("null")
	public static int doubleRange(double min, double max, double input) {
		double total_length = max - min;
		double subrange_length = total_length / 10;

		double current_start = 0.7;
		for (int i = 0; i < 10; ++i) {
			System.out.println("Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
			if (Double.compare(input, min) > 0 && Double.compare(input, max) < 0) {
				return i;
			} else {
				System.out.println("Not in Range");
			}
			current_start += 1;
		}
		return (Integer) null;
	}

	@SuppressWarnings("null")
	public static int DateRange(int minYear, int maxYear, int minMonth, int maxMonth, int minDay, int maxDay,
			int inputYear, int inputMonth, int inputDay) {
		double total_length = (maxYear - minYear);
		double subrange_length = (total_length / 10) + 0.1;
		double current_start = minYear;

		double total_length1 = (maxMonth - minMonth);
		double subrange_length1 = (total_length1 / 10) + 0.1;
		double current_start1 = minMonth;

		double total_length2 = (maxDay - minDay);
		double subrange_length2 = (total_length2 / 10) + 0.1;
		double current_start2 = minDay;

		for (int i = 0; i < 10; ++i) {
			System.out.println("Smaller range: [" + (int) current_start + "-" + (int) current_start1 + "-"
					+ (int) current_start2 + ", " + (int) (current_start + subrange_length) + "-"
					+ (int) (current_start1 + subrange_length1) + "-" + (int) (current_start2 + subrange_length2)
					+ "]");

			SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
			Date d1 = null;
			Date d2 = null;
			Date d3 = null;
			try {
				d1 = sdformat.parse((int) current_start + "-" + (int) current_start1 + "-" + (int) current_start2);
				d2 = sdformat.parse((int) (current_start + subrange_length) + "-"
						+ (int) (current_start1 + subrange_length1) + "-" + (int) (current_start2 + subrange_length2));
				d3 = sdformat.parse(inputYear + "-" + inputMonth + "-" + inputDay);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			/*
			 * System.out.println(sdformat.format(d1) + "----"); System.out.println(d2 +
			 * "----"); System.out.println(d3 + "----");
			 */

			if (sdformat.format(d3).compareTo(sdformat.format(d1)) > 0
					&& sdformat.format(d3).compareTo(sdformat.format(d2)) < 0) {
				return i;
			} else {
				System.out.println("Not in Range");
			}
			current_start += subrange_length;
			current_start1 += subrange_length1;
			current_start2 += subrange_length2;
		}
		return (Integer) null;
	}

	public void checkConstraint(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		Table t = null;
		try {
			t = deserializeTable(strTableName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		Set<String> set = htblColNameValue.keySet();
		Iterator<String> iterator = set.iterator();
		while (iterator.hasNext()) {
			String columnName = iterator.next();
			if (!t.columnsName.contains(columnName)) {
				throw new DBAppException("column " + columnName + " doesn't exist in table " + strTableName);
			}
		}
	}

	public static boolean checkMinMax(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws ClassNotFoundException, IOException, ParseException {
		Table table = deserializeTable(strTableName);
		Hashtable<String, String> max = table.max;
		Hashtable<String, String> min = table.min;
		Vector<String> columnsName = table.columnsName;

		for (int i = 0; i < columnsName.size(); i++) {

			System.out.println(htblColNameValue + " Here");
			System.out.println(columnsName.get(i));
			System.out.println(htblColNameValue.get(columnsName.get(i)) + " END");
			if (htblColNameValue.get(columnsName.get(i)).getClass().getSimpleName().equals("Date")) {
				continue;
			}
			if (htblColNameValue.get(columnsName.get(i)).getClass().getSimpleName().equals("Integer")) {
				Integer temp = Integer.parseInt(htblColNameValue.get(columnsName.get(i)).toString());
				Integer temp2 = Integer.parseInt(max.get(columnsName.get(i)));
				Integer temp3 = Integer.parseInt(min.get(columnsName.get(i)));
				if (Integer.compare(temp, temp2) > 0)
					return false;
				if (Integer.compare(temp, temp3) < 0)
					return false;
			} else {
				String temp = htblColNameValue.get(columnsName.get(i)).toString();
				String temp2 = max.get(columnsName.get(i));
				String temp3 = min.get(columnsName.get(i));
				if (temp.compareTo(temp2) > 0)
					return false;
				if (temp.compareTo(temp3) < 0)
					return false;
			}
		}
		return true;
	}

	public static boolean checkMinMaxForUpdate(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws ClassNotFoundException, IOException, ParseException {
		Table table = deserializeTable(strTableName);
		Hashtable<String, String> max = table.max;
		Hashtable<String, String> min = table.min;
		Vector<String> columnsName = table.columnsName;

		for (int i = 0; i < columnsName.size(); i++) {

			System.out.println(htblColNameValue + " Here");
			System.out.println(columnsName.get(i));
			System.out.println(htblColNameValue.get(columnsName.get(i)) + " END");
			if (columnsName.get(i).equals(table.PrimaryKey))
				continue;
			if (htblColNameValue.get(columnsName.get(i)).getClass().getSimpleName().equals("Date")) {
				continue;
			}
			if (htblColNameValue.get(columnsName.get(i)).getClass().getSimpleName().equals("Integer")) {
				Integer temp = Integer.parseInt(htblColNameValue.get(columnsName.get(i)).toString());
				Integer temp2 = Integer.parseInt(max.get(columnsName.get(i)));
				Integer temp3 = Integer.parseInt(min.get(columnsName.get(i)));
				if (Integer.compare(temp, temp2) > 0)
					return false;
				if (Integer.compare(temp, temp3) < 0)
					return false;
			} else {
				String temp = htblColNameValue.get(columnsName.get(i)).toString();
				String temp2 = max.get(columnsName.get(i));
				String temp3 = min.get(columnsName.get(i));
				if (temp.compareTo(temp2) > 0)
					return false;
				if (temp.compareTo(temp3) < 0)
					return false;
			}
		}
		return true;
	}

	public static int searchBucketOverFlowIndex(Table t, Bucket b, Object PK) {
		int index = b.getPKs().indexOf(PK);
		int indexOfBucketOverFlow = 0;
		int j = 0;
		for (Object s : b.getPKs()) {
			if (index >= (j * 100) && index < ((j + 1) * 100)) {
				indexOfBucketOverFlow = j;
				break;
			}
			j++;
		}
		return indexOfBucketOverFlow;
	}

	public static int searchPageIndex(Table t, Object value) {
		int index = t.getPKs().indexOf(value);
		int indexOfPage = 0;
		int j = 0;
		for (String s : t.getPageNames()) {
			if (index >= (j * 250) && index < ((j + 1) * 250)) {
				indexOfPage = j;
				break;
			}
			j++;
		}
		return indexOfPage;
	}

	private static void serializePages(Page page, Table t1, int pageIndex) {
		try {
			FileOutputStream fileOut = new FileOutputStream(
					"src/main/resources/data/" + t1.getName() + "Page" + pageIndex);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(page);
			out.close();
			fileOut.close();
		} catch (Exception i1) {
			i1.printStackTrace();
		}
	}

	private static void serializeTables(Table t1) {
		try {
			FileOutputStream fileOut = new FileOutputStream("src/main/resources/data/" + "Table " + t1.getName());
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(t1);
			out.close();
			fileOut.close();
		} catch (Exception i1) {
			i1.printStackTrace();
		}
	}

	private static Table deserializeTable(String tableName) throws IOException, ClassNotFoundException {
		Table t;
		FileInputStream fis = new FileInputStream(new File("src/main/resources/data/" + "Table " + tableName));
		ObjectInputStream in = new ObjectInputStream(fis);
		t = (Table) in.readObject();
		in.close();
		fis.close();
		return t;
	}

	private static Page deserializePages(Table t, int index) throws IOException, ClassNotFoundException {
		Page p = null;
		FileInputStream fis = new FileInputStream(new File("src/main/resources/data/" + t.getName() + "Page" + index));
		ObjectInputStream in = new ObjectInputStream(fis);
		p = (Page) in.readObject();
		in.close();
		fis.close();
		return p;
	}

	private static Page serializeOverFlow(Page page, Table t1, int OverFlowIndex) {
		try {
			FileOutputStream fileOut = new FileOutputStream(
					"src/DB2Project/src/main/resources/data/" + t1.getName() + "OverFlow" + OverFlowIndex);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(page);
			out.close();
			fileOut.close();
		} catch (Exception i1) {
			i1.printStackTrace();
		}
		return page;
	}

	private static Page deserializeOverFlowPage(Table t, int index) throws IOException, ClassNotFoundException {
		Page overflow = null;
		FileInputStream fis = new FileInputStream(
				new File("src/DB2Project/src/main/resources/data/" + t.getName() + "OverFlow" + index));
		ObjectInputStream in = new ObjectInputStream(fis);
		overflow = (Page) in.readObject();
		in.close();
		fis.close();
		return overflow;
	}

	@SuppressWarnings("rawtypes")
	public static boolean isSubset(Vector arr1, Vector arr2) {
		int i = 0;
		int j = 0;
		for (i = 0; i < arr2.size(); i++) {
			for (j = 0; j < arr1.size(); j++)
				if (arr2.get(i).equals(arr1.get(j)))
					break;
			if (j == arr1.size())
				return false;
		}
		return true;
	}

	public static void main(String[] args) throws Exception {
		String strTableName = "students";
		DBApp dbApp = new DBApp();

//		Hashtable htblColNameValue = new Hashtable();
//		htblColNameValue.put("id", new Integer( 2343432 ));
//		htblColNameValue.put("name", new String("Ahmed Noor" ) );
//		htblColNameValue.put("gpa", new Double( 0.95 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );

//		SQLTerm[] arrSQLTerms;
//		arrSQLTerms = new SQLTerm[2];
//		arrSQLTerms[0] = new SQLTerm();
//		arrSQLTerms[0]._strTableName = "students";
//		arrSQLTerms[0]._strColumnName = "id";
//		arrSQLTerms[0]._strOperator = "=";
//		arrSQLTerms[0]._objValue = "76-8563";
//		arrSQLTerms[1] = new SQLTerm();
//		arrSQLTerms[1]._strTableName = "students";
//		arrSQLTerms[1]._strColumnName = "name";
//		arrSQLTerms[1]._strOperator = "=";
//		arrSQLTerms[1]._objValue = "Khaled";
//		String[] strarrOperators = new String[1];
//		strarrOperators[0] = "XOR";
//		Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
//		while (resultSet.hasNext()) {
//			Row r = (Row) resultSet.next();
//			System.out.println(r.values);
//		}

//		Table t = deserializeTable(strTableName);
//		System.out.println(t.getPageNames());
//		Page p = deserializePages(t, 0);
//		System.out.println(t.getPKs());
//		System.out.println(p.getRow().get(0).values);
//		System.out.println(p.getRow().get(1).values);
//		System.out.println(p.getRow().get(2).values);
//		System.out.println(p.getRow().get(3).values);
//		System.out.println(p.getRow().get(4).values);
//		System.out.println(p.getRow().get(5).values);

//		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
//		htblColNameType.put("id", "java.lang.String");
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("gpa", "java.lang.Double");
//		dbApp.init();
//		Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();
//		Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
//		htblColNameMax.put("id", "99-9999");
//		htblColNameMin.put("id", "43-0000");
//		htblColNameMax.put("name", "ZZZZZZZZZZ");
//		htblColNameMin.put("name", "A");
//		htblColNameMax.put("gpa", "5.0");
//		htblColNameMin.put("gpa", "0.7");
//		dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);
////////
		Hashtable htblColNameValue = new Hashtable();
//		htblColNameValue.put("id", new String("56-8563"));
		htblColNameValue.put("gpa", new Double(2.5));
//		htblColNameValue.put("name", new String("Yousef"));
		dbApp.deleteFromTable(strTableName, htblColNameValue);
//		dbApp.updateTable(strTableName, "76-8563", htblColNameValue);

//		 dbApp.createIndex(strTableName, new String[] { "id", "gpa" });
//		System.out.println(deserializeTable(strTableName).GridNames.toStrings());
//		
////		System.out.println(deserializeTable(strTableName).getKeysWithIndex());
////		System.out.println(deserializeTable(strTableName).getColumnsName());
//		System.out.println(deserializeTable(strTableName).GridIndeces.get(0).getBucketNames());
//		System.out.println(deserializeTable(strTableName).GridIndeces.get(1).getBucketNames());

//		System.out.println(deserializeTable(strTableName).GridIndeces.get(0).getName());
//////		System.out.println(deserializeTable(strTableName).GridIndeces.get(1)
////				.getIndexes());
//		System.out.println(deserializeTable(strTableName).GridIndeces.get(1).getName());
		// String[] array = { "Khaled", "Omar", "Yousef" };
		// System.out.println(getPermutations(array, new
		// Vector<String>()).toString());
		// int[] arr = {10, 20, 30, 40, 50};
		// Arrays.copyOfRange(arr, 0, 2); // returns {10, 20}
		// Arrays.copyOfRange(arr, 1, 4); // returns {20, 30, 40}
		// Arrays.copyOfRange(arr, 2, arr.length); // returns {30, 40, 50}
		// (length = 5)
	}
}