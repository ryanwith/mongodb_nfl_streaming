from urllib.parse import quote_plus
from datetime import datetime

def generate_connection_string(username, password, app_name, cluster_url):
    encoded_username = quote_plus(username)
    encoded_password = quote_plus(password)
    uri = "mongodb+srv://"
    uri += encoded_username + ":" + encoded_password + "@" + cluster_url
    uri += "/?retryWrites=true&w=majority&appName=" + app_name
    return uri


def convert_dates(document):
    """Convert string datetime fields to proper datetime objects"""
    # Convert time_of_day
    if 'time_of_day' in document and document['time_of_day']:
        document['time_of_day'] = datetime.strptime(document['time_of_day'], "%Y-%m-%dT%H:%M:%SZ")
    return document

def format_documents(documents):
    formatted_docs = []
    for doc in documents:
        if doc['time_of_day'] == None:
            continue
        else:
            formatted_docs.append(convert_dates(doc))
    return formatted_docs

