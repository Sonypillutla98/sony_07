import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, concat_ws, sha2, col,lit,current_date, xxhash64, concat
from pyspark.sql.types import DecimalType,datetime

spark = SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").appName('myapp').getOrCreate()
spark.sparkContext.addPyFile("s3://sony-landingzone-batch07/files/delta-core_2.12-0.8.0.jar")
from delta import *

class SparkDataTransformationJob:
    def __init__(self, spark):
        self.spark = spark
       
    def read_data(self, source_path):
        return self.spark.read.format("parquet").load(source_path)

    def transform_data(self, data, transform_columns, mask_fields):

    # Apply decimal transformations
        for column in transform_columns.get("decimal_type", []):
            data = data.withColumn(column, col(column).cast("decimal(10, 7)"))

    # Apply string transformations
        for column in transform_columns.get("string_type", []):
            data = data.withColumn("new_column", split(column," "))
            data = data.withColumn(column, concat_ws(",", data["new_column"])).drop("new_column")

    # Mask specified columns
        for column in mask_fields:
            if column in data.columns:
                newcolumn = 'masked_'+column
                data = data.withColumn(newcolumn, sha2(col(column), 256))
        return data

    def write_data(self, data, destination_path, partition_cols=None):
        if partition_cols:
            data.write.format("parquet").mode("overwrite").partitionBy(partition_cols).save(destination_path)
        else:
            data.write.format("parquet").mode("overwrite").save(destination_path)
    def create_lookuptable(self, data):
        try:
            #check the delta table exists or not
            DeltaTable.forPath(spark, "s3://sony-stagingzone-batch07/delta_Table")
            print("deltaTable exist.")
            self.delta_table_exists = True
        
        except:
            print("deltaTable does not exist.")
            self.delta_table_exists = False
        
        if self.delta_table_exists:
            targetTable = DeltaTable.forPath(spark, "s3://sony-stagingzone-batch07/delta_Table")
            targetDF = spark.read.format('delta').load("s3://sony-stagingzone-batch07/delta_Table")
            sourceDF = data.select("advertising_id","user_id","masked_advertising_id","masked_user_id").withColumn("start_date", lit(current_date())).withColumn("end_date", lit('0000-00-00'))
            joindf = sourceDF.join(targetDF,(sourceDF.advertising_id==targetDF.advertising_id)&(sourceDF.user_id==targetDF.user_id)&(targetDF.flag_active=="Y"),"leftouter").select(sourceDF["."],targetDF.advertising_id.alias("t_advertising_id"),targetDF.user_id.alias("t_user_id"),targetDF.masked_advertising_id.alias("t_masked_advertising_id"),targetDF.masked_user_id.alias("t_masked_user_id"))
            filterDF = joindf.filter(xxhash64(joindf.masked_advertising_id,joindf.masked_user_id)!=xxhash64(joindf.t_masked_advertising_id,joindf.t_masked_user_id))
            mergeDF= filterDF.withColumn("MERGEKEY", concat(filterDF.advertising_id, filterDF.user_id))
            dummyDF=filterDF.filter("t_advertising_id is not null").withColumn("MERGEKEY", lit(None))
            scdDF=mergeDF.union(dummyDF)            
            targetTable.alias("target").merge(source = scdDF.alias("source"),condition = "concat(target.advertising_id,target.user_id) = source.MERGEKEY and target.flag_active = 'Y' ").whenMatchedUpdate(set={"flag_active":"'N'","end_date":"current_date"}).whenNotMatchedInsert(values={"advertising_id": "source.advertising_id","user_id":"source.user_id","masked_advertising_id":"source.masked_advertising_id","masked_user_id":"source.masked_user_id","flag_active":"'Y'","start_date":"current_date","end_date":"""to_date('9999-12-31','yyyy-MM-dd')"""}).execute()
        else:    
            targetDF = data.select("advertising_id","user_id","masked_advertising_id","masked_user_id").withColumn("flag_active", lit("Y")).withColumn("start_date", lit(current_date())).withColumn("end_date", lit('None'))
            targetDF.write.format('delta').mode('overwrite').save("s3://sony-stagingzone-batch07/delta_Table")
            
    def run_job(self, config):
        actives_landing_zone_path = config["actives_file_s3bucket_paths"]["landing_zone_bucket"]
        actives_raw_zone_path = config["actives_file_s3bucket_paths"]["raw_zone_bucket"]
        actives_staging_zone_path = config["actives_file_s3bucket_paths"]["staging_zone_bucket"]

        viewership_landing_zone_path = config["viewership_file_s3bucket_paths"]["landing_zone_bucket"]
        viewership_raw_zone_path = config["viewership_file_s3bucket_paths"]["raw_zone_bucket"]
        viewership_staging_zone_path = config["viewership_file_s3bucket_paths"]["staging_zone_bucket"]

        mask_fields = config["mask-columns"]
        actives_file_transform = config["actives_file_transform-columns"]
        viewership_file_transform = config["viewership_file_transform-columns"]

        partition_cols = config["partition-columns"]

        landing_data_actives = self.read_data(actives_landing_zone_path)
        landing_data_viewership = self.read_data(viewership_landing_zone_path)
        
        """Copying data from landing zone to raw zone"""
        #self.write_data(landing_data_actives, actives_raw_zone_path)
        #self.write_data(landing_data_viewership, viewership_raw_zone_path)

        raw_data_actives = self.read_data(actives_raw_zone_path)
        raw_data_viewership = self.read_data(viewership_raw_zone_path)

        transformed_data_actives = self.transform_data(raw_data_actives, actives_file_transform, mask_fields)
        transformed_data_viewership = self.transform_data(raw_data_viewership, viewership_file_transform, mask_fields)
        
        self.create_lookuptable(transformed_data_actives)
        
        """Writing transformed data by patitioning to staging zone"""
        self.write_data(transformed_data_actives, actives_staging_zone_path, partition_cols)
        self.write_data(transformed_data_viewership, viewership_staging_zone_path, partition_cols)
        


if __name__ == "__main__":
    #spark = SparkSession.builder.appName("SparkJob").getOrCreate()
    
    configData = spark.sparkContext.textFile("s3://sony-landingzone-batch07/configs/app_config.json").collect()
    data = ''.join(configData)
    config_details = json.loads(data)

    job = SparkDataTransformationJob(spark)
    job.run_job(config_details)
