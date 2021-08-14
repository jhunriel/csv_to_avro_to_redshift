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


def campaigns(campaign, rewards, txns):
    conn, cur = con()
    cur.execute("truncate table dev.public.campaign;")
    cur.execute("truncate table dev.public.reward_campaigns;")
    cur.execute("truncate table dev.public.reward_transactions;")
    print("Successfully Truncated")

    for index, row in campaign.iterrows():
        cur.execute(f"""insert into dev.public.campaign
        (id, name, status)
        values ({row["Id"]},'{row["name"]}','{row["status"]}');""")
    print("Campaign Successfully Inserted")

    for index, row in rewards.iterrows():
        updated_at = row["updated_at"][:-4]
        cur.execute(f"""insert into dev.public.reward_campaigns
        (id, reward_name, updated_at,campaign_id)
        values ({row["id"]},'{row["reward_name"]}','{updated_at}','{row["campaign_id"]}');""")
    print("Rewards Successfully Inserted")

    for index, row in txns.iterrows():
        updated_at = row["updated_at"][:-4]
        cur.execute(f"""insert into dev.public.reward_transactions
        (id, status, updated_at,reward_campaign_id)
        values ({row["id"]},'{row["status"]}','{updated_at}','{row["reward_campaign_id"]}');""")
    print("Transactions Successfully Inserted")
    conn.commit()
    conn.close()
    print("Query is commited")


if __name__ == "__main__":
    raw_campaign = "https://media.githubusercontent.com/media/PerxTech/data-interview/master/data/campaign.csv"
    raw_reward_campaign = "https://media.githubusercontent.com/media/PerxTech/data-interview/master/data/reward_campaign.csv"
    raw_reward_transactions = "https://media.githubusercontent.com/media/PerxTech/data-interview/master/data/reward_transaction.csv"
    df_campaign = pd.read_csv(raw_campaign)
    df_rewards = pd.read_csv(raw_reward_campaign)
    df_transactions = pd.read_csv(raw_reward_transactions)
    campaigns(df_campaign, df_rewards, df_transactions)
