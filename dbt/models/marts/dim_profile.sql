with profile as (
    
    select * from {{ ref('stg_scholar_profile') }}

),
metrices as (

    select * from {{ ref('stg_scholar_metrice') }}

),
all_profile as (
    select 
        profile.author_id,
        profile.author_name,
        profile.author_affiliations,
        profile.profile_link,
        metrices.citations_count,
        metrices.cited_year,
        metrices.h_index_count,
        metrices.i10_index_count
    from
        profile left join metrices
            on profile.author_id = metrices.author_id

    where profile.verified_email = 'verified email at mq.edu.au' and metrices.cited_year = 'all'
)

select * from all_profile