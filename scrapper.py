import os
import pandas as pd
from dotenv import load_dotenv
from serpapi import GoogleSearch



def fetch_and_save_scholar_data():
    # Load the .env file and get the API key
    load_dotenv()
    API_KEY = os.environ.get('api_key')

    # Get scholar data
    page_tokens = ['CcwDAHAS__8']
    scholars = []

    for page_token_id in page_tokens:
        params = {
            "api_key": API_KEY,
            "engine": "google_scholar_profiles",
            "mauthors": "Macquarie University",
            "hl": "en",
            "after_author": page_token_id
        }

        search = GoogleSearch(params)
        result = search.get_dict()
        profile = result.get("profiles", None)

        if profile:
            scholars.append(profile)

    flattened_scholars = [item for sublist in scholars for item in sublist]
    scholar_list = pd.DataFrame(flattened_scholars)[['author_id','name', 'affiliations', 'link', 'email', 'cited_by']]

    # Get articles and metrics
    results = []
    years_of_interest = range(1980, 2024)

    citations_info = []
    author_metrics = []

    for author_id in scholar_list['author_id']:
        params = {
            "api_key": API_KEY,
            "engine": "google_scholar_author",
            "author_id": author_id
        }

        search = GoogleSearch(params)
        result = search.get_dict()
        results.append(result)

    for result in results:
        author_id = result['search_parameters']['author_id']

        for year_data in result['cited_by']['graph']:
            year = year_data['year']
            if year in years_of_interest:
                citations_info.append({
                    'author_id': author_id,
                    'year': year,
                    'citations': year_data['citations']
                })

        years_to_extract = ['all', 'since_2018']

        for year in years_to_extract:
            author_metrics.append({
                'author_id': author_id,
                'year': year,
                'h_index': result['cited_by']['table'][1]['h_index'][year],
                'i10_index': result['cited_by']['table'][2]['i10_index'][year],
                'citations': result['cited_by']['table'][0]['citations'][year]
            })

        years_present = [data['year'] for data in result['cited_by']['graph']]
        for year in years_of_interest:
            if year not in years_present:
                citations_info.append({
                    'author_id': author_id,
                    'year': year,
                    'citations': 0
                })

    citations_by_year = pd.DataFrame(citations_info)
    author_metrics_df = pd.DataFrame(author_metrics)

    # Save DataFrames to CSV files
    scholar_list.to_csv('gg_scholar_list.csv', index=False, sep=';')
    citations_by_year.to_csv('gg_scholar_citations_by_year.csv', index=False, sep=';')
    author_metrics_df.to_csv('gg_scholar_author_metrics.csv', index=False, sep=';')


# Call the function
#fetch_and_save_scholar_data()