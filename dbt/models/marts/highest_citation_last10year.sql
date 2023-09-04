--top 5 researcher in the last 10 years

With tenyearcitation as (
    select 
        author_id, 
        author_name, 
        sum(citations_count) as count

    from {{ ref('fct_achievement') }} 
    where cited_year between 2014 and 2023
    group by author_id, author_name
)

select * from tenyearcitation
order by count desc
limit 5