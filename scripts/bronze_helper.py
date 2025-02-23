from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

class BronzeLoader:
    def __init__(self, entity_name: str):
        self.entity_name = entity_name

        self.base_path = spark.sql(" SELECT url FROM system.information_schema.external_locations WHERE external_location_name = 'landing_ext' ").select("url").collect()[0][0]

        self.bronze_path = spark.sql(" SELECT url FROM system.information_schema.external_locations WHERE external_location_name = 'bronze_ext' ").select("url").collect()[0][0]

        self.checkpoint_path = f"{self.bronze_path}checkpoints/{self.entity_name}_checkpoint"


        # self.schema = f"{self.entity_name}_schema"
        

    def get_source_path(self) -> str:
        return f"{self.base_path}/{self.entity_name}"
    
    def get_schema(self):
        return f"{self.entity_name}_schema"

    def get_autoloader_options(self) -> dict:
        return {
            "cloudFiles.format": "csv",
            "cloudFiles.schemaLocation": self.checkpoint_path,
            "cloudFiles.useIncrementalListing": "auto",
            "header": "true",
            "cloudFiles.inferColumnTypes": "true"

        }

    def load_to_bronze(self) -> DataFrame:
        return spark.readStream.format("cloudFiles") \
            .options(**self.get_autoloader_options()) \
            .load(self.get_source_path()) \
            .withColumn("ExtractedTime", current_timestamp())




    def write_to_bronze(self, df: DataFrame) -> None:
        write_data= (df.writeStream.format('delta')\
                    .option("checkpointLocation", self.checkpoint_path)\
                    .outputMode('append')\
                    .queryName('loadToBronze')\
                    .trigger(availableNow=True)\
                    .toTable(f"`freelance_poc_catalog`.`ecom_bronze`.`{self.entity_name}`"))
        
        write_data.awaitTermination()
        
     