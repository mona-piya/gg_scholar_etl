with profile as (

    --Patched William L. Griffin double shcholar accounts - select max value from both accounts--
    select * from {{ ref('dim_profile') }}  where author_name != 'William L. Griffin'
    UNION ALL
    select
        max(author_id),
        max(author_name),
        max(author_affiliations),
        max(profile_link),
        max(citations_count),
        max(cited_year),
        max(h_index_count),
        max(i10_index_count)
    from {{ ref('dim_profile') }} 
    where author_name='William L. Griffin'
    group by author_name

),
citation as (
    
    select * from {{ ref('dim_citation') }} 

),

all_achv as (
    select  
        profile.author_id,
        profile.author_name,
        profile.author_affiliations,
        profile.profile_link,
        profile.citations_count as all_citations,
        profile.h_index_count,
        profile.i10_index_count,
        citation.cited_year,
        citation.citations_count

    from
        profile left join citation
            on profile.author_id = citation.author_id
)

select * from all_achv 


