from numpy import column_stack
import pandas as pd
import os
import psycopg2


def con():
    conn = psycopg2.connect(
        host=os.environ["HOST"],
        user=os.environ["USERNAME"],
        port=os.environ["PORT"],
        password=os.environ["PASSWORD"],
        dbname=os.environ["DATABASE"])
    cur = conn.cursor()
    return conn, cur


def campaigns(campaign):
    conn, cur = con()
    cur.execute("truncate table dev.public.campaign_reward_mapping;")
    print("Successfully Truncated")

    for index, row in campaign.iterrows():
        cur.execute(f"""insert into dev.public.campaign_reward_mapping
        (campaign_id,reward_id)
        values ({row["campaign_id"]},{row["reward_id"]});""")
    print("Campaign Successfully Inserted")
    conn.commit()
    conn.close()
    print("Query is commited")


if __name__ == "__main__":
    raw_campaign = "https://media.githubusercontent.com/media/PerxTech/data-interview/master/data/campaign_reward_mapping.csv"
    df_campaign = pd.read_csv(raw_campaign)
    campaigns(df_campaign)
