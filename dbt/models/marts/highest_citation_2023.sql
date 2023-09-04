--top5 researcher in 2023
select author_id, author_name, cited_year, citations_count
from {{ ref('fct_achievement') }} 
where cited_year = 2023
order by citations_count desc
limit 5