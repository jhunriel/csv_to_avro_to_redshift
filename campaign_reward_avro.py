import pandas as pd
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter


def avrowriter(s, avrofile):
    schema = avro.schema.parse(open(s, "rb").read())
    writer = DataFileWriter(open(avrofile, "wb"), DatumWriter(), schema)
    return writer


if __name__ == '__main__':
    httplog = 'https://media.githubusercontent.com/media/PerxTech/data-interview/master/data/http_log.txt'
    df = pd.read_csv(httplog, header=None, delim_whitespace=True)
    records = len(df)-1
    writer = avrowriter('campaign.avsc', 'campaign_reward.avro')
    for row in range(records):
        writer.append({"timestamp": df[0][row], "http_method": df[1]
                      [row], "http_path": df[2][row], "user_id": str(df[3][row])})
    writer.close()
    print("Successfully Converted to Avro")
