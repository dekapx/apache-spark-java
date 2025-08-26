# Apache Spark 4 & Java 21 

### EmptyDataFrameWithSchema
```java
- Create an empty DataFrame with a schema
- Display the empty DataFrame
```

### AddDataToEmptyDataFrameWithSchema
```java
- Create an empty DataFrame with a schema
- Display the empty DataFrame
- Add values to the DataFrame
- Display the DataFrame with values
```

### CsvReadDataFrameInferSchema
```java
- Read a CSV file into a DataFrame with schema inference
- CSV contents are separated by pipes ("|")
- Display the DataFrame
```
```json
schema sample
{
  "id": "IntegerType",
  "first_name": "StringType",
  "middle_name": "StringType",
  "last_name": "StringType",
  "date_of_birth": "DateType",
  "city": "StringType",
  "marks": "DoubleType"
}
```

### WriteDataFrameWithSchemaToCsv
```java
- Create an empty DataFrame with a schema
- Display the empty DataFrame
- Add values to the DataFrame
- Display the DataFrame with values
- Write the DataFrame to a CSV file
```
