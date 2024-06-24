--Tại mỗi năm chỉ lưu trữ ra 3 tháng có có Millions of Dollars cao nhất trong năm đó (value giảm dần)
with ranked as(
	select *,row_number() over (partition by extract(year from aggregation_date) order by millions_of_dollar desc) as rank_num
	from consumption_alcoholic_20240618
)
select
	category,
	sub_category,
	aggregation_date,
	millions_of_dollar,
	pipeline_exc_datetime
from ranked
where rank_num <= 3
order by extract(year from aggregation_date) desc, millions_of_dollar desc

--=================================================
--Sắp xếp thứ thứ tổng thể thì năm nào có tổng Millions_of_Dollars (cao nhất) thì lên đầu tiên 
select 
	extract(year from aggregation_date), 
	SUM(millions_of_dollar) AS year_total
from consumption_alcoholic_20240618
group by extract(year from aggregation_date)
order by year_total desc