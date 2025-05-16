import itertools, json, pandas
from jsonschema import validate
import numpy
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType
from pyspark.sql.functions import current_timestamp, lit
import vertexai
from vertexai.generative_models import GenerativeModel, Part

region = "us-central1"
model_name = "gemini-2.0-flash-001"
prompt = """Here is a list of names.
I want you to check of the name corresponds to an American Football Stadium.
if it does, return the name of the stadium, the state and city it is from, its full address, the year it opened,,
whether it has an open, closed, or retractable roof, its capacity, its longitude, and its latitude.
Return the reuslts as a list of JSON objects with the schema [{"name" : string, "location" : string, "address" : string, "opening_date" : string, "roof_type" : string, "capacity" : integer, "longitude" : float, "latitude" : float}].
Return only one answer per stadium.
Do include stadiums that have already been closed.
Do not include commas in the capacity field.
Return the opening_date field as a string.
Return Longitude and Latitude as a signed floating point number.
the roof_type field should be entirely lowercase.
Don't return the records which are not American football stadiums.
Don't return any empty JSON objects.
Don't return an explanation for your answer.

Here are some sample runs:

I give you:
"Fenway Park, Boston, MA"
"SoFi Stadium, Inglewood, CA"
"Allegiant Stadium, Paradise, NV"

You return:
{"name": "SoFi Stadium", "location": "Inglewood, CA", "address": "1001 S. Stadium Dr. Inglewood, CA 90301", "opening_date": "September 8, 2020", "roof_type": "Open", "capacity": 70000, "longitude": "118.3390 W", "Latitude": "33.9535 N"}
{"name": "Allegiant Stadium", "location": "Las Vegas, NV","address": "3333 Al Davis Way Las Vegas, NV 89118", "opening_date": "July 31, 2020","roof_type": "Closed", "capacity": 65000, "longitude": "115.1833 W", "latitude": "36.0909 N"}
"""
def enrich(input_str):
    results = []
    vertexai.init(location = region)
    model = GenerativeModel(model_name)
    resp = model.generate_content([input_str, prompt])

    resp_text = resp.text.replace("```json", "").replace("```", "").replace("\n", "")
    try:
        results = json.loads(resp_text)
        json_schema = {"type" : "object",
            "properties" : {
            "name" : {"type" : "string"},
            "location" : {"type" : "string"},
            "address" : {"type" : "string"},
            "opening_date" : {"type" : "string"},
            "roof_type" : {"type" : "string"},
            "capacity" : {"type" : "number"},
            "longitude" : {"type" : "number"},
            "latitude" : {"type" : "number"},
            },
        }
        for obj in results:
            validate(obj, json_schema)

    except Exception as e:
        print("Error while parsing json:", e, ".  The error was caused by: ", resp_text)
        return []
    return results

def model(dbt, session):
    input_df = dbt.ref("tmp_stadiums")
    num_stadiums = input_df.count()
    print("number of stadiums to process: ", num_stadiums)
    
    batch_size = 30
    num_batches = int(num_stadiums / batch_size)
    combined_results = []

    pandas_df = input_df.select("name", "location").filter("name is not null and location is not null").toPandas()
    batches = numpy.array_split(pandas_df, num_batches)

    for i in range(num_batches):
        subset_stadiums = batches[i].to_string(header = False)
        results = enrich(subset_stadiums)
        combined_results.extend(results)
    
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("address", StringType(), True),
        StructField("opening_date", StringType(), True),
        StructField("roof_type", StringType(), True),
        StructField("capacity", IntegerType(), True),
        StructField("longitude", FloatType(), True),
        StructField("latitude", FloatType(), True)
        ])

    output_df = session.createDataFrame(combined_results, schema)
    output_df = output_df.withColumn("_data_source", lit("Kaggle"))
    output_df = output_df.withColumn("_load_time", lit(current_timestamp()))
    num_stadiums = output_df.count()

    print("number of stadiums returned:  ", num_stadiums)

    return output_df
            
