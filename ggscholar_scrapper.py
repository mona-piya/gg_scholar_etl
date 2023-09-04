import os
import pandas as pd
import re
from serpapi import GoogleSearch
from dotenv import load_dotenv
import boto3
from io import StringIO

# Load the .env file
load_dotenv()

# Get the API key from the environment variable
API_KEY = os.environ.get('api_key')

s3 = boto3.client(
    's3',
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))   

BUCKET_NAME = 'mona-bucket-23'


def fetch_scholars_data():
    page_tokens = ['CcwDAHAS__8','CcwDAHAS__8J','s2n0AMaS__8J','wpgAAEuj__8J','70p0AFe8__8J','HNoDAOzF__8J','NGwEACfN__8J','S58BAMLP__8J','H4NLAJ_T__8J','EOMAAPTY__8J']
    # size s page_tokens = ['CcwDAHAS__8','CcwDAHAS__8J','s2n0AMaS__8J','wpgAAEuj__8J','70p0AFe8__8J']

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

    # Flatten the nested list of dictionaries structure
    flattened_scholars = [item for sublist in scholars for item in sublist]

    # Extract interests from the flattened list
    for scholar in flattened_scholars:
        scholar['interests'] = ', '.join([interest['title'] for interest in scholar.get('interests', [])])

    # Create a DataFrame and select only the required columns in the desired order
    scholar_list = pd.DataFrame(flattened_scholars)[['author_id','name', 'affiliations', 'link', 'email', 'cited_by', 'interests']]



    return scholar_list


def extract_publication_details(publication):
    """Extracts journal, volume, issue, and pages from the publication string."""
    # Pattern explanation:
    # ^(.+?)\s: Capture everything at the start until a space (the journal name).
    # (?=\d+(\s?\(\w+\))?): Look ahead for a number (volume) followed optionally by a space and a bracketed issue number.
    journal_pattern = r"^(.+?)\s(?=\d+(\s?\(\w+\))?)"
    # Look for the first group of digits (volume number).
    volume_pattern = r"(\d+)(?=(\s?\(\w+\))?,)"
    # Look for bracketed digits or letters (issue number).
    issue_pattern = r"\((\w+)\)"
    # Look for a comma followed by a space and then capture the page range.
    pages_pattern = r",\s(\w+-\w+)"
    
    journal = re.search(journal_pattern, publication)
    volume = re.search(volume_pattern, publication)
    issue = re.search(issue_pattern, publication)
    pages = re.search(pages_pattern, publication)

    return {
        "journal": journal.group(1).lower() if journal else None,
        "volume": volume.group(1) if volume else None,
        "issue": issue.group(1) if issue else None,
        "pages": pages.group(1) if pages else None
    }

def fetch_articles_and_metrics(scholar_list):

    results = []
    article_list = []
    count=0
    for author_id in scholar_list['author_id']:
        print("fetching: ", author_id)
        count += 1
        params = {
            "api_key": API_KEY,
            "engine": "google_scholar_author",
            "author_id": author_id,
            "num": "5" #Number of journal articles return
        }

        search = GoogleSearch(params)
        result = search.get_dict()
        article = result.get("articles", None)

        if result:
            results.append(result)

        if article:
            article_list.append(article)
    
    print("Completed fetching: ", count, "scholars")

    # Extracting the required fields from the data
    all_articles = []
    for group in article_list:
        for article in group:
            #pub_details = extract_publication_details(article.get('publication', ''))
            extracted = {
                'title': article.get('title', None),
                'link': article.get('link', None),
                'citation_id': article.get('citation_id', None),
                #'article_id': article.get('citation_id', "").split(':')[1] if 'citation_id' in article else None,
                #'author_id': article.get('citation_id', "").split(':')[0] if 'citation_id' in article else None,
                'authors': article.get('authors', None),
                'publication': article.get('publication', None),
                #'journal': pub_details["journal"],
                #'volume': pub_details["volume"],
                #'issue': pub_details["issue"],
                #'pages': pub_details["pages"],
                'year': article.get('year', None),
                'cited_by_value': article.get('cited_by', {}).get('value', None)
            }
            all_articles.append(extracted)

    # Convert the list of extracted articles to a DataFrame
    all_articles = pd.DataFrame(all_articles)


    years_of_interest = range(1980, 2024)  # Considering years from 1980 to 2023

    citations_info = []
    author_metrics = []

    for result in results:
        author_id = result['search_parameters']['author_id']
        for year_data in result['cited_by']['graph']:
            year = year_data['year']

            if year in years_of_interest:
                citations = year_data['citations']
                citation_data = {
                    'author_id': author_id,
                    'year': year,
                    'citations': citations
                }
                citations_info.append(citation_data)

        years_to_extract = ['all', 'since_2018']

        for year in years_to_extract:
            h_index = result['cited_by']['table'][1]['h_index'][year]
            i10_index = result['cited_by']['table'][2]['i10_index'][year]
            citations = result['cited_by']['table'][0]['citations'][year]

            metric_info = {
                'author_id': author_id,
                'year': year,
                'h_index': h_index,
                'i10_index': i10_index,
                'citations': citations
            }
            author_metrics.append(metric_info)

    for result in results:
        author_id = result['search_parameters']['author_id']
        years_present = [data['year'] for data in result['cited_by']['graph']]
        for year in years_of_interest:
            if year not in years_present:
                citations_info.append({
                    'author_id': author_id,
                    'year': year,
                    'citations': 0
                })

    citations_by_year = pd.DataFrame(citations_info)
    author_metrics = pd.DataFrame(author_metrics)

    return citations_by_year, author_metrics, all_articles

def save_df_to_s3(dataframe, filename):
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, index=False, sep='|')
    s3.put_object(Bucket=BUCKET_NAME, Body=csv_buffer.getvalue(), Key=filename)
    

if __name__ == "__main__":
    scholar_list = fetch_scholars_data()
    citations_by_year, author_metrics, all_articles = fetch_articles_and_metrics(scholar_list)

    #Save DataFrames to CSV files
    scholar_list.to_csv('gg_scholar_list.csv', index=False, sep='|')
    citations_by_year.to_csv('gg_scholar_citations_by_year.csv', index=False, sep='|')
    author_metrics.to_csv('gg_scholar_author_metrics.csv', index=False, sep='|')
    all_articles.to_csv('gg_scholar_article.csv', index=False, sep='|')

    # Save DataFrames to S3
    save_df_to_s3(scholar_list, 'gg_scholar_list.csv')
    save_df_to_s3(citations_by_year, 'gg_scholar_citations_by_year.csv')
    save_df_to_s3(author_metrics, 'gg_scholar_author_metrics.csv')
    save_df_to_s3(all_articles, 'gg_scholar_article.csv')

    print("Successfully saved the dataframes to S3")


