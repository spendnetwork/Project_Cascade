import sys
import os
from google.api_core.client_options import ClientOptions
from google.cloud import automl_v1beta1 as automl
from google.cloud.automl_v1beta1.proto import service_pb2
from google.protobuf.json_format import MessageToDict
import pdb
import pandas as pd
from dotenv import load_dotenv

#
# def inline_text_payload(string):
#
#   return {'text_snippet': {'content': string, 'mime_type': 'text/plain'} }
#
#
# def get_prediction(row, model_name):
#   options = ClientOptions(api_endpoint='automl.googleapis.com')
#   prediction_client = automl_v1beta1.PredictionServiceClient(client_options=options)
#
#   # payload = inline_text_payload(row.string)
#   payload = inline_text_payload(row)
#   # Uncomment the following line (and comment the above line) if want to predict on PDFs.
#   # payload = pdf_payload(file_path)
#
#   params = {}
#
#   response = prediction_client.predict(model_name, payload, params)
#
#   respdict = MessageToDict(response)
#
#   label = ''
#   score = 0
#
#   for i in respdict['payload']:
#     if i['classification']['score'] > score:
#       score = i['classification']['score']
#       label = i['displayName']
#
#   row.at['Label'] = label
#   row.at['Score'] = score
#
#   return row

def get_prediction(row, model_name, project_id, model_id, client, content):

  prediction_client = automl.PredictionServiceClient()

  model_full_id = client.model_path(
    project_id, compute_region, model_id
  )

  with open(content, "rb") as content_file:
    snippet = content_file.read()


if __name__ == '__main__':
  pdb.set_trace()
  load_dotenv()

  # https: // cloud.google.com / natural - language / automl / docs / tutorial
  # python googleAutoML.py '/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/orgtype-classifier/data/AutoMLAPI/uk_data_entity.csv' projects/724416479180/locations/us-central1/models/TCN5514574180831461376
  content = sys.argv[1]
  model_name = sys.argv[2]
  compute_region = os.environ(['REGION_NAME'])
  client = automl.AutoMlClient()

  df = pd.read_csv(content)
  dfstrings = df['entity_name']

  dfstrings = dfstrings.apply(get_prediction, model_name=model_name, client=client, compute_region=compute_region)

  dfjoin = pd.merge(df, dfstrings, left_on='entity_name', right_on='entity_name',how='left')
  dfjoin.to_csv('./data/ukdataentity_classd.csv', index=False)
