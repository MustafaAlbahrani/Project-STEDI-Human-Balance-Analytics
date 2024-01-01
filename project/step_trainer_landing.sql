{
  "Name": "step_trainer_landing",
  "DatabaseName": "database",
  "Description": "",
  "CreateTime": "2023-12-29T04:45:24.000Z",
  "UpdateTime": "2023-12-29T04:45:24.000Z",
  "Retention": 0,
  "StorageDescriptor": {
    "Columns": [
      {
        "Name": "sensorreadingtime",
        "Type": "float",
        "Comment": ""
      },
      {
        "Name": "serialnumber",
        "Type": "string",
        "Comment": ""
      },
      {
        "Name": "distancefromobject",
        "Type": "float",
        "Comment": ""
      }
    ],
    "Location": "s3://udacity-project-data-lake/step_trainer/landing/",
    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
    "Compressed": false,
    "NumberOfBuckets": 0,
    "SerdeInfo": {
      "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"
    },
    "SortColumns": [],
    "StoredAsSubDirectories": false
  },
  "PartitionKeys": [],
  "TableType": "EXTERNAL_TABLE",
  "Parameters": {
    "classification": "json"
  },
  "CreatedBy": "arn:aws:sts::705179886550:assumed-role/voclabs/user2537378=cd176802-45c2-11ed-bc43-c343c2786dce",
  "IsRegisteredWithLakeFormation": false,
  "CatalogId": "705179886550",
  "IsRowFilteringEnabled": false,
  "VersionId": "0",
  "DatabaseId": "195ccf1f762c4cd4b63bdfa5290a050a",
  "IsMultiDialectView": false
}