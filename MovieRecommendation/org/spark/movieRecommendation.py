from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *



spark=SparkSession.builder.appName("MOvie Recommendation System").master("local").getOrCreate()

df=spark.read.option("header","true") \
    .csv("C:\\data\\MovieRecommendation\\movies_metadata.csv")\
    .drop("poster_path","production_companies","production_countries","homepage").dropDuplicates()#.printSchema()#.head()
fillMissingVal=df.na.fill(value="0")
#df1=missingVal.exceptAll(df)
#fillMissingVal.select("genres").show()
#Convert json column to multiple columns


#formatting json data for generes column
# genres_schema = StructType([
#     StructField('id',StringType(),True),
#     StructField('name',StringType(),True)
# ])
# json_schema = ArrayType(genres_schema)# Define the schema for the JSON column as an array of objects
# dfJSON = fillMissingVal.withColumn("jsonData",from_json(col("genres"),json_schema))
# extracted_df = dfJSON.select("jsonData.id", "jsonData.name")#.show(truncate=False)
# dict_column = map_from_arrays(col("id"),col("name")).alias("dictionary")#creating a dictionary column
# df_with_dict = extracted_df.select("*", dict_column)
#
# #formatting json data for spoken_languages column
# spoken_languages_schema=StructType([
#     StructField('iso_639_1',StringType(),True),
#     StructField('name',StringType(),True),
# ])
# spokenLangArray=ArrayType(spoken_languages_schema)
# spokenLangDF=fillMissingVal.withColumn("jsonData",from_json(col("spoken_languages"),spokenLangArray))
# spokenLangDF_Extracted = spokenLangDF.select("jsonData.iso_639_1", "jsonData.name")#.show(truncate=False)
# dict_column = map_from_arrays(col("iso_639_1"),col("name")).alias("dictionary")#creating a dictionary column
# df_with_dict = spokenLangDF_Extracted.select("*", dict_column).show()
def formatJsonCol(col_name,**kwargs):
    l1=[]
    l2=[]
    l3=[]
    for key, value in kwargs.items():
        if(value=='string'):
            value=StringType()
        elif(value=="int"):
            value=IntegerType()
        print(key, value)
        l1.append(StructField(key,value,True))
        #l2.append(f"jsonData.{key}")
        l3.append(f"col(\"{key}\")")
        l3.append(f"{key}")
    # col_schema=StructType(l1)
    # return StructType(l1)
    # Convert list to a single string
    single_string = ','.join(l2)
    single_string2 = ','.join(l3)
    print(single_string)
    print(str(single_string2).split(','))


    dfArray=ArrayType(StructType(l1))
    dfjson=fillMissingVal.withColumn("jsonData",from_json(col(col_name),dfArray))
    spokenLangDF_Extracted = dfjson.select(single_string.split(','))#.show(truncate=False)
    dict_column = map_from_arrays().alias("dictionary")#creating a dictionary column
    df_with_dict = spokenLangDF_Extracted.select("*", dict_column).show()

formatJsonCol("spoken_languages",iso_639_1='string',name='string')
















