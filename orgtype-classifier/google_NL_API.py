from google.cloud import language_v1
from google.cloud.language_v1 import enums
import pdb
import argparse
from google.protobuf.json_format import MessageToDict, MessageToJson
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def load_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--datadir', default='./data')
    parser.add_argument('--datafile', type=str, default='2019-11-11_matches.csv')
    args = parser.parse_args()
    filename = os.path.splitext(args.datafile)[0]
    parser.add_argument('--outfile', type=str, default=filename + '_googleentities.csv')
    args = parser.parse_args()
    return args


def load_data(args):
    df = pd.read_csv(os.path.join(args.datadir, args.datafile))
    return df


def save_data(df, args):
    df.to_csv(os.path.join(args.datadir, args.outfile))


def initialise_google_NL_api():
    client = language_v1.LanguageServiceClient()
    return client


def analyze_entities(row, client):
    """
    Analyzing Entities in a String

    Args:
      text_content The text content to analyze
    """
    # Get address from row object (from datafile)
    text_content = row.src_address_adj

    if pd.notnull(text_content):
        # Google API configs
        type_ = enums.Document.Type.PLAIN_TEXT
        language = "en"
        document = {"content": text_content, "type": type_, "language": language}
        encoding_type = enums.EncodingType.UTF8

        # Call to client
        response = client.analyze_entities(document, encoding_type=encoding_type)

        # Convert response to dictionary (can't use json.loads for google response)
        respdict = MessageToDict(response)
        for i in range(len(respdict['entities'])):
            resptype = respdict['entities'][i]['type']
            if resptype == 'ADDRESS':
                address_data = respdict['entities'][i]['metadata']
                # Assign to row object and return to dataframe
                for key, value in address_data.items():
                    row.at[key] = value
                return row


def main():

    args = load_args()

    df = load_data(args)

    client = initialise_google_NL_api()
    df = df[:10]
    df = df.apply(analyze_entities, client=client, axis=1)
    save_data(df, args)



if __name__ == '__main__':

    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/davidmellor/.ssh/Spend Network-ed68e5166fbd.json'
    main()

    # sample_analyze_entities("3rd floor county hall oxford ox1 1nd united kingdom High School")