import itertools, json, pandas
from jsonschema import validate
import numpy
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType
from pyspark.sql.functions import current_timestamp, lit
import vertexai
from vertexai.generative_models import GenerativeModel, Part

region = "us-central1"
model_name = "gemini-2.0-flash-001"
prompt = """Here is a list of names of American football stadiums.
  I want you to take the name of the stadium and return each stadium's name, its average review score on Google, as well as how many Superbowls that stadium has hosted.
  Return the results as a list of properly formatted JSON objects.
  Return only one answer per stadium.
  You must return the public review score of each stadium found on Google.
  You must return how many superbowl games each stadium has hosted.
  Format each element in the list as a JSON object with the schema:
  {"name": string, "review_score": float, "superbowls_hosted": integer}
  return the name as a string.
  return the review score as a float.
  round the review score to one decimal point.
  return the superbowl count as an integer.
  do not return any null values for review_score.
  do not return any null values for superbowl_count.
  if you cannot find the review score of a stadium, return 0.
  do not include an explanation with your answer.
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
                "review_score" : {"type" : "number"},
                "superbowls_hosted" : {"type" : "number"},
            },
        }
        for obj in results:
            validate(obj, json_schema)

    except Exception as e:
        print("Error while parsing json:  ", e, ".  The error was caused by:  ", resp_text)
        return []
    print(results)
    return results

def model(dbt, session):
    input_df = dbt.ref("tmp_stadiums_enrichment")
    num_stadiums = input_df.count()
    print("number of stadiums to process: ", num_stadiums)

    batch_size = 30
    num_batches = int(num_stadiums / batch_size)
    combined_results = []
    pandas_df = input_df.select("name").filter("name is not null").toPandas()
    batches = numpy.array_split(pandas_df, num_batches)

    for i in range(num_batches):
        subset_stadiums = batches[i].to_string(header = False)
        results = enrich(subset_stadiums)
        combined_results.extend(results)
    
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("review_score", FloatType(), True),
        StructField("superbowls_hosted", IntegerType(), True)
    ])

    output_df = session.createDataFrame(combined_results, schema)
    num_stadiums = output_df.count()

    print("number of stadiums returned:  ", num_stadiums)

    return output_df
