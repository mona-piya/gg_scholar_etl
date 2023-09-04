with source as (

    select * from {{ source('scholar', 'gg_scholar_metrices') }}

),

renamed as (

    select
        {{ dbt_utils.surrogate_key(
            ['author_id', 'year']) }} 
            as metrice_key,
        author_id as author_id,
        year as cited_year,
        h_index as h_index_count,
        i10_index as i10_index_count,
        citations as citations_count

    from source

)

select * from renamed