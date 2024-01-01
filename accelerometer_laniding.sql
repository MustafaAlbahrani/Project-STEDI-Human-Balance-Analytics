{
  "Name": "accelerometer_laniding",
  "DatabaseName": "database",
  "Owner": "hadoop",
  "CreateTime": "2023-12-28T22:32:44.000Z",
  "UpdateTime": "2023-12-28T22:32:44.000Z",
  "LastAccessTime": "1970-01-01T00:00:00.000Z",
  "Retention": 0,
  "StorageDescriptor": {
    "Columns": [
      {
        "Name": "user",
        "Type": "string"
      },
      {
        "Name": "timestamp",
        "Type": "float"
      },
      {
        "Name": "x",
        "Type": "float"
      },
      {
        "Name": "y",
        "Type": "float"
      },
      {
        "Name": "z",
        "Type": "float"
      }
    ],
    "Location": "s3://udacity-project-data-lake/accelerometer/landing",
    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
    "Compressed": false,
    "NumberOfBuckets": -1,
    "SerdeInfo": {
      "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
      "Parameters": {
        "mapping": "TRUE",
        "serialization.format": "1",
        "ignore.malformed.json": "FALSE",
        "dots.in.keys": "FALSE",
        "case.insensitive": "TRUE"
      }
    },
    "BucketColumns": [],
    "SortColumns": [],
    "Parameters": {},
    "SkewedInfo": {
      "SkewedColumnNames": [],
      "SkewedColumnValues": [],
      "SkewedColumnValueLocationMaps": {}
    },
    "StoredAsSubDirectories": false
  },
  "PartitionKeys": [],
  "TableType": "EXTERNAL_TABLE",
  "Parameters": {
    "EXTERNAL": "TRUE",
    "classification": "json",
    "transient_lastDdlTime": "1703802764"
  },
  "CreatedBy": "arn:aws:sts::705179886550:assumed-role/voclabs/user2537378=cd176802-45c2-11ed-bc43-c343c2786dce",
  "IsRegisteredWithLakeFormation": false,
  "CatalogId": "705179886550",
  "IsRowFilteringEnabled": false,
  "VersionId": "0",
  "DatabaseId": "195ccf1f762c4cd4b63bdfa5290a050a",
  "IsMultiDialectView": false
}