{
  "Name": "customer_curated",
  "DatabaseName": "database",
  "Owner": "hadoop",
  "CreateTime": "2023-12-29T00:34:38.000Z",
  "UpdateTime": "2023-12-29T00:34:38.000Z",
  "LastAccessTime": "1970-01-01T00:00:00.000Z",
  "Retention": 0,
  "StorageDescriptor": {
    "Columns": [
      {
        "Name": "customername",
        "Type": "string"
      },
      {
        "Name": "email",
        "Type": "string"
      },
      {
        "Name": "phone",
        "Type": "string"
      },
      {
        "Name": "brithday",
        "Type": "string"
      },
      {
        "Name": "serialnumber",
        "Type": "string"
      },
      {
        "Name": "registerationdate",
        "Type": "float"
      },
      {
        "Name": "lastupatedate",
        "Type": "float"
      },
      {
        "Name": "sharewithresearchasofdate",
        "Type": "float"
      },
      {
        "Name": "sharewithpublicasofdate",
        "Type": "float"
      }
    ],
    "Location": "s3://udacity-project-data-lake/customer/curated",
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
    "transient_lastDdlTime": "1703810078"
  },
  "CreatedBy": "arn:aws:sts::705179886550:assumed-role/voclabs/user2537378=cd176802-45c2-11ed-bc43-c343c2786dce",
  "IsRegisteredWithLakeFormation": false,
  "CatalogId": "705179886550",
  "IsRowFilteringEnabled": false,
  "VersionId": "0",
  "DatabaseId": "195ccf1f762c4cd4b63bdfa5290a050a",
  "IsMultiDialectView": false
}