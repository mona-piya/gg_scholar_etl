with source as (

    select * from {{ source('scholar', 'gg_scholar_list') }}

),

renamed as (

    select

        author_id as author_id,
        name as author_name,
        affiliations as author_affiliations,
        link as profile_link,
        lower(email) as verified_email,
        cited_by as total_citation

    from source

)

select * from renamed