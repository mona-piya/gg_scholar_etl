with
    source as (select * from {{ source("scholar", "gg_scholar_citation") }}),

    renamed as (

        select
            {{ dbt_utils.surrogate_key(
                ['author_id', 'cited_year']) }}
                as citation_key,
            author_id as author_id,
            cited_year as cited_year,
            citations as citations_count

        from source

    )

select *
from renamed
