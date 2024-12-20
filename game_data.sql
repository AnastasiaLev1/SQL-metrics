with ttl as (
    select *, ROW_NUMBER() over (partition by user_id, game_name, payment_date, revenue_amount_usd order by payment_date) as rn
    from project.games_payments
)
, 
full_tabl as (-- check for duplicates, merge tables
	select distinct t.user_id 
	   ,t.game_name
	   ,t.payment_date
	   ,date_trunc('month', t.payment_date)::date as month_date
	   ,t.revenue_amount_usd
	   ,p.language 
	   ,p.has_older_device_model 
	   ,p.age
from ttl t
left join project.games_paid_users p using (user_id, game_name)
where rn = 1
)
,  
repeated_users as (-- Recurring revenue sources 
	select distinct user_id
	from full_tabl t
	group by user_id
	having count (distinct month_date) > 1
)
,
ttl_users as ( -- paid users, new paid users, churned users, MRR
	select ft.user_id
		,ft.month_date
		,max(ft.payment_date) over (partition by ft.user_id) as max_date
		,min(ft.payment_date) over (partition by ft.user_id) as min_date
		,case when ft.revenue_amount_usd > 0 then ft.user_id end as paid_users
		,case when ft1.user_id is null then ft.user_id end as new_paid_users
		,case when ft2.user_id is null then ft.user_id end as churned_users 
		,sum(case when ft.user_id in (select user_id from repeated_users)
			    then ft.revenue_amount_usd 
			    else 0 end) over (partition by ft.user_id, ft.payment_date) as MRR 
	from full_tabl ft
	left join full_tabl ft1 on ft.user_id = ft1.user_id
			and ft.month_date = ft1.month_date - interval '1 month'
	left join full_tabl ft2 on ft.user_id = ft2.user_id
			and ft.month_date = ft2.month_date + interval '1 month'
)
,ttl_all as (
select distinct ft.user_id 
  	  ,tu.paid_users
  	  ,tu.new_paid_users
   	  ,tu.churned_users
   	  ,case when ft.user_id = tu.churned_users then lag(ft.revenue_amount_usd) over (partition by ft.user_id order by ft.month_date) else 0 end as churned_revenue
      ,tu.MRR
      ,sum(case when ft.user_id = tu.new_paid_users 
   			    then ft.revenue_amount_usd 
			    else 0 end) over (partition by ft.user_id, ft.payment_date) as New_MRR 
	  ,case when (ft.revenue_amount_usd - lag(ft.revenue_amount_usd) over (partition by ft.user_id order by ft.month_date)) > 0 
   			then (tu.MRR - lag(tu.MRR) over (partition by ft.user_id, ft.month_date)) 
    		else 0 end as expansion_MRR 
      ,case when (ft.revenue_amount_usd - lag(ft.revenue_amount_usd) over (partition by ft.user_id order by ft.month_date)) < 0 
   			then (lag(tu.MRR) over (partition by ft.user_id, ft.month_date) - tu.MRR) 
    		else 0 end as contraction_MRR
	  ,sum(case when ft.user_id = tu.churned_users then ft.revenue_amount_usd else 0 end) over (partition by ft.user_id) as LTV
	  ,avg(case when ft.user_id = tu.churned_users then age(tu.max_date, tu.min_date) end) over (partition by ft.user_id) as LT
      ,ft.game_name
      ,ft.month_date
      ,ft.revenue_amount_usd
      ,ft.language 
      ,ft.has_older_device_model 
      ,ft.age
from full_tabl ft
left join ttl_users tu on ft.user_id = tu.user_id
			and ft.month_date = tu.month_date
)
select month_date
,count (distinct paid_users) paid_users
,count (distinct new_paid_users) new_paid_users
,count (distinct churned_users) churned_users
,sum(churned_revenue) churned_revenue
,max(MRR) MRR
,max(New_MRR) New_MRR
,sum(expansion_MRR) expansion_MRR
,sum(contraction_MRR) contraction_MRR
from ttl_all 
group by 1