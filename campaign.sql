----------- CREATE MASTER TABLE -----------------
create table dev.public.campaign_transactions as
select a.id
        ,a.status as reward_status
    ,a.updated_at as transaction_date_utc
    ,convert_timezone('SGT',a.updated_at) as transaction_date_sgt
    ,b.reward_name
    ,b.updated_at as campaign_date
    ,c.name as campaign_name
    ,c.status as campaign_status
    ,date_trunc('hour',convert_timezone('SGT',a.updated_at)) as floor_date_sgt
    ,datepart(w,convert_timezone('SGT',a.updated_at)) as week
from reward_transactions a left join reward_campaigns b on a.reward_campaign_id=b.id
    left join campaign c on b.campaign_id=c.id;


--------  A) SQL Test Use Case 1: ------------
 
select count(*) as No_of_Transactions
	,date(floor_date_sgt) as Date
	,floor_date_sgt::time as Time
from campaign_transactions
where transaction_date_sgt between '2019-08-12 19:12:00' and '2019-08-16 19:0:00'
group by 2,3
order by 1 desc




--------  A) SQL Test Use Case 2: ------------
select 
	 a.reward_name
    ,a.redeem_cnt
    ,((a.redeem_cnt-b.prev_redeem_cnt)::float/b.prev_redeem_cnt)*100 as percent_diff
    ,a.week
from (select reward_name,week,count(reward_status) as redeem_cnt
	  from campaign_transactions 
	  where reward_status='redeemed'
	  group by 1,2
	  order by 1,2) a inner join  (select reward_name,week as prev_week,count(reward_status) as prev_redeem_cnt
                                    from campaign_transactions 
                                    where reward_status='redeemed'
                                    group by 1,2
                                    order by 1,2) b
        on a.reward_name=b.reward_name and a.week-1=b.prev_week

