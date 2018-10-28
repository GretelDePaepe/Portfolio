#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Dec  2 11:32:24 2017
@author: GDP
"""
# %% Import libs used in main

import datetime
import os
import sys
import pymongo
from bson.objectid import ObjectId
import pandas as pd
import numpy as np
from sklearn.externals import joblib

# %% Our functions


def connect_mongo(database):
    """
    Creates a connection to a mongo client and database
    Parameters
    ----------
    database: str
        Name of the database
    """
    from pymongo import MongoClient
    client = MongoClient()  # Making a connection with MongoClient
    db = eval('client.%s' % database)  # Making a connection with a database
    return db


def get_new_investigations(db, collection):
    """
    Description
    ----------
    Obtains which investigations should be run from the mongodb
    Parameters
    ----------
    db: object returned by function connect_mongo(database)
        A mongodb database object
    collection: string
        The name of a mongodb collection
    Returns
    -------
    out : if there are no pending investigations, the value returned is
          "Nothing to investigate", which will terminate the script
          else the functions returns a list of dictionaries each containing
          the following:
            investigation_id: Mongo Object ID for the investigation
            investigation_requestor: User id of the person who requested
            the investigation
            investigation_date_requested: Date and time of the request
            investigation_subject_group: The subject group(s) selected
            investigation_topic_group: The topic group(s) selected
    Examples
    --------
    >>> get_new_investigations(db, 'Run')
    >>> [ {'investigation_date_requested': Timestamp('2018-04-09 15:00:00'),
           'investigation_id': ObjectId('5acb3d132bf6256a300602f8'),
           'investigation_requestor': u'Mirela',
           'investigation_subject_group': [u'Bank Melli'],
           'investigation_topic_group': [u'Money Laundering']},
           {'investigation_date_requested': Timestamp('2018-04-09 08:00:00'),
           'investigation_id': ObjectId('5acb35df2bf6256a300602f7'),
           'investigation_requestor': u'Gretel',
           'investigation_subject_group': [u'Bank Melli'],
           'investigation_topic_group': [u'Fraud']},
           {'investigation_date_requested': Timestamp('2018-04-09 01:00:00'),
           'investigation_id': ObjectId('5acb6ace2bf6256a300602fb'),
           'investigation_requestor': u'SYSTEM',
           'investigation_subject_group': u'All',
           'investigation_topic_group': u'All'}]
    """
    collection = eval('db.%s' % collection)
    cursor = collection.find({'InvestigationStatus': {'$exists': False},
                              'ScheduledDateTime':
                                  {'$lt': datetime.datetime.now()}})
    investigations = pd.DataFrame(list(cursor))
    if len(investigations) == 0:
        return "Nothing to investigate"
    else:
        investigation_list = []
        for i in range(len(investigations)):
            investigation_dict = {}
            investigation_dict['investigation_id'] = investigations['_id'][i]
            investigation_dict['investigation_requestor'] = \
                investigations['Investigator'][i]
            investigation_dict['investigation_date_requested'] = \
                investigations['ScheduledDateTime'][i]
            investigation_dict['investigation_subject_group'] = \
                investigations['SubjectGroup'][i]
            investigation_dict['investigation_topic_group'] = \
                investigations['TopicGroup'][i]
            investigation_list.append(investigation_dict)
        return investigation_list


def define_runs(investigation_list):
    """
    Description
    ----------
    Defines how many times the main block needs to be executed to cover all
    the investigations scheduled.
    The aim here is to avoid any duplication in case of multiple investigations
    in order not to incur more API call costs than required.
    Parameters
    ----------
    investigation_list: list
        result of get_new_investigations(db, collection)
    Returns
    -------
    out : dict
        A dict with all unique runs required to cover all investigations.
        In case all topic groups for all subject groups was included in one of
        the investigations, only one run
        will be defined with all subjects and all topics.
    Examples
    --------
    >>> define_runs(investigation_list)
    >>> {u'Abanka dd': [u'Legal'],
         'All': [u'Fraud'],
         u'Bank Melli': [u'AML & Sanctions']}
    """
    run_dict = {}
    topic_group_all = []
    for investigation in investigation_list:
        if "All" in investigation['investigation_subject_group']:
            topic_group_all.append(investigation['investigation_topic_group'])
            topic_group_all = list(set([item for sublist in topic_group_all
                                        for item in sublist]))
    if len(topic_group_all) > 0:
        run_dict['All'] = topic_group_all
        if 'All' in run_dict['All']:
            return run_dict
    for investigation in investigation_list:
        subject_group = investigation['investigation_subject_group']
        for s in subject_group:
            if not run_dict.get(s):
                topic_group = []
                for investigation in investigation_list:
                    subject_group_sub = \
                        investigation['investigation_subject_group']
                    if s in subject_group_sub:
                        topic_group.append(
                                investigation['investigation_topic_group'])
                topic_group = list(set([item for sublist in topic_group
                                        for item in sublist]))
                topic_group = [t for t in topic_group
                               if t not in topic_group_all]
                run_dict[s] = topic_group
    return run_dict


def change_investigation_status(db, collection, investigation_list, status):
    """
    Description
    ----------
    Updates the status of the investigation(s)
    Parameters
    ----------
    db: object returned by function connect_mongo(database)
        A mongodb database object
    collection: string
        The name of a mongodb collection
    investigation_list: list
        Results of get_new_investigations(db, collection)
    status : string
        The name of the status: "Scraping the www", "Predicting usefulness
        using AI", "Searching nearest neighbors using AI",
        "Summarizig investigation and finally "Investigation Completed"
    Returns
    -------
    out : nothing is returned
    Examples
    --------
    >>> change_investigation_status(db,
                                    'Investigation',
                                    investigation_list,
                                    "Scraping the www")
    >>> change_investigation_status(db,
                                    'Investigation',
                                    investigation_list,
                                    "Predicting usefulness using AI")
    """
    if len(investigation_list) > 0:
        investigation_id_list = [i['investigation_id']
                                 for i in investigation_list]
        collection = eval('db.%s' % collection)
        collection.update_many({'_id': {'$in': investigation_id_list}},
                               {'$set': {'InvestigationStatus': status}})


def get_topics(db, collection, topic_group=['All']):
    """
    Gets all the topics from the mongodb database Topic
    The topics are the key words we are searching for
    Parameters
    ----------
    db: object returned by function connect_mongo(database)
        A mongodb database object
    collection: string
        The name of a mongodb collection
    topic_group: ['All'] (default) or other list
        When "All" all the topics are returned
        When list, only the topics which fall in the groups specified in the
        list are returned
    Returns
    -------
    out: list
         [u'fraud',
          u'money laundering',
          u'terrorist financing',
          ...
          u'abuse',
          u'sanctions',
          u'acquisitions']
    """
    collection = eval('db.%s' % collection)
    if topic_group == ['All']:
        cursor = collection.find({}, {'_id': 0, 'Topic': 1})
    else:
        cursor = collection.find({'TopicGroup_id': {'$in': topic_group}},
                                 {'_id': 0, 'Topic': 1})
    topics = [t['Topic'].lower() for t in list(cursor)]
    return topics


def get_subjects(db, collection, subject_group='All'):
    """
    Gets all the subjects from the mongodb database Subject
    The subjects are the clients we are researching
    Parameters
    ----------
    db: object returned by function connect_mongo(database)
        A mongodb database object
    collection: string
        The name of a mongodb collection
    subject_group: All (default) or str
        When "All" all the subjects are returned
        When str, only the subjects which fall in the group specified in the
        str are returned
    Returns
    -------
    out: list
         [u'bank melli',
         u'ay',
         u'abanka dd',
         u'melli bank',
         u'abanka']
    WARNING
    --------
    The names of the companies used in this prototype are selected at random
    and have no connection whatsoever with the participants of this project.
    """
    collection = eval('db.%s' % collection)
    if subject_group == 'All':
        cursor = collection.find({}, {'_id': 0, 'Subject': 1})
    else:
        cursor = collection.find({'SubjectGroup_id': subject_group},
                                 {'_id': 0, 'Subject': 1})
    subjects = [s['Subject'].lower() for s in list(cursor)]
    return subjects


def get_rules(db, collection):
    """
    Gets all the rules from the mongodb database Rule
    For rule based exclusion (or inclusion) of certain news items.
    Parameters
    ----------
    db: object returned by function connect_mongo(database)
        A mongodb database object
    collection: string
        The name of a mongodb collection
    Returns
    -------
    out: DataFrame
            RuleType               Terms                _id
         0  Exclude url including  [wikipedia, bmi.ir]  5a9709efbeb547550b24...
    Other
    --------
    Each rule will need to be mapped to a Python function.
    For example the "Exclude url including" rule, maps to the
    exclude_url_including() function
    Not comfortable this has been fully thought through.
    Actual usage of the tool might lead to a better way of thinking here.
    """
    import pandas as pd
    collection = eval('db.%s' % collection)
    cursor = collection.find()
    rules = pd.DataFrame(list(cursor))
    return rules


def exclude_url_including(db):
    """
    Calls the get_rules() function to collect the rules from the mongodb Rule
    Filters for the rules of type "Exclude url including"
    Returns a list of words we will search for in the url since we want to
    exclude those urls containing these words
    Parameters
    ----------
    db: object returned by function connect_mongo(database)
        A mongodb database object
    Returns
    -------
    out: list
        Returns a list of words we will search for in the url since we want to
        exclude those urls containing these words
    Examples
    --------
    >>> exclude_url_including()
    >>> [u'wikipedia', u'bmi.ir', u'abanka.si']
    We do not want for example urls which have wikipedia in the url or
    which include the domain of the company we are researching, since the
    latter is owned by the company and is not considered objective.
    """
    rules = get_rules(db, 'Rule')
    relevant_rules = rules[rules['RuleType'] == 'Exclude url including']
    terms_to_exclude_from_url = [r.lower() for r in relevant_rules['Terms'][0]]
    return terms_to_exclude_from_url


def news_api(subjects, topics, path):
    """
    Uses newsapi.org to collect news articles on the subjects and topics
    Parameters
    ----------
    subjects: list
        A list of companies for which we want to capture the news
    topics: list
        A list of topics we want to search on
    path: home dir of the tenant
    Returns
    -------
    out: urlDict
        It returns a dictionary with unique urls for further examination, as
        well as metadata related to these urls, such as
    Examples
    --------
    >>> news_api(['Bank Melli', 'Abanka dd'], ['fraud', 'money laundering'])
    >>> {u'http://www.dailymail.co.uk/w...port.html':
        {'dateshtml': u'2017-12-09T12:10:16Z',
         'headline': u'Iran sentences fugitive ex-bank chief to jail: report',
         'source': {'author': u'http://www.dailymail..., By Afp',
         'detail': {u'id': u'daily-mail', u'name': u'Daily Mail'},
         'source': 'NewsAPI'},
         'summary': u'Iran has handed down ...breaking $2.6 billion...'}}
    To do
    --------
    - Perhaps we should do something with responses['status'] if it returns
    something else than 'ok'.
    - Since the app is under development the usage of the API is free,
    but we need to mention the url below on our webpage "Powered by..."
    See Also
    --------
    https://newsapi.org/
    """
    from newsapi import NewsApiClient
    import datetime
    import pickle  # Using pickle since I do not want the key visible on Github
    urlDictApi = {}
    to = datetime.datetime.today().strftime('%Y-%m-%d')
    from_year = str(datetime.datetime.today().year - 5)  # Not older than 5y
    frm = from_year + to[4:]
    news_api_key = pickle.load(open(path + "NewsApiKey.p", "rb"))
    newsapi = NewsApiClient(api_key=news_api_key)
    for subject in subjects:
        subject = subject.lower()
        for topic in topics:
            topic = topic.lower()
            qry = topic + ' ' + subject
            responses = newsapi.get_everything(q=qry,
                                               from_parameter=frm,
                                               to=to,
                                               language='en')
            if len(responses['articles']) > 0:
                for response in responses['articles']:
                    if response['url'] not in urlDictApi:
                        url = response['url']
                        responseDict = {}
                        responseDict['summary'] = response['description']
                        responseDict['headline'] = response['title']
                        responseDict['dateshtml'] = response['publishedAt']
                        sourceDict = {}
                        sourceDict['source'] = 'NewsAPI'
                        sourceDict['author'] = response['author']
                        sourceDict['detail'] = response['source']
                        responseDict['source'] = sourceDict
                        urlDictApi[url] = responseDict
    return urlDictApi


def news_api_subjects_only(subjects, no_of_calls, path):
    """
    Uses newsapi.org to collect news articles on the subjects
    Parameters
    ----------
    subjects: list
        A list of subjects for which we want to capture the news
    no_of_calls: the number of calls made so far to the API
    path: home dir of the tenant
    Returns
    -------
    out: urlDict
        It returns a dictionary with unique urls for further examination, as
        well as metadata related to these urls
       : no_of_calls
        New number of calls made to the API so far
    Examples
    --------
    >>> news_api_subjects_only(['Bank Melli', 'Abanka dd'], 1024)
    >>> 1032, {u'http://www.dailymail.co.uk/w...port.html':
        {'dateshtml': u'2017-12-09T12:10:16Z',
         'headline': u'Iran sentences fugitive ex-bank chief to jail: report',
         'source': {'author': u'http://www.dailymail..., By Afp',
         'detail': {u'id': u'daily-mail', u'name': u'Daily Mail'},
         'source': 'NewsAPI'},
         'summary': u'Iran has handed down ...breaking $2.6 billion...'}}
    See Also
    --------
    https://newsapi.org/
    """
    from newsapi import NewsApiClient
    import datetime
    import pickle  # Using pickle since I do not want the key visible on Github
    urlDictApi = {}
    to = datetime.datetime.today().strftime('%Y-%m-%d')
    from_year = str(datetime.datetime.today().year - 5)  # Not older than 5y
    frm = from_year + to[4:]
    news_api_key = pickle.load(open(path + "NewsApiKey.p", "rb"))
    newsapi = NewsApiClient(api_key=news_api_key)
    for subject in subjects:
        subject = subject.lower()
        responses = newsapi.get_everything(q=subject,
                                           from_parameter=frm,
                                           to=to,
                                           language='en')
        no_of_calls += 1
        if len(responses['articles']) > 0:
            for response in responses['articles']:
                if response['url'] not in urlDictApi:
                    url = response['url']
                    responseDict = {}
                    responseDict['summary'] = response['description']
                    responseDict['headline'] = response['title']
                    responseDict['dateshtml'] = response['publishedAt']
                    sourceDict = {}
                    sourceDict['source'] = 'NewsAPI'
                    sourceDict['author'] = response['author']
                    sourceDict['detail'] = response['source']
                    responseDict['source'] = sourceDict
                    urlDictApi[url] = responseDict
    return no_of_calls, urlDictApi


def check_url_exists(url, db, collection):
    """
    Function to check if a particular url already exists in the mongodb
    We ignore whether it is a http or https url, by only taking into account
    the part after //
    Parameters
    ----------
    url: string
        A url for example 'https://www.bbc.com/....html'
    db: object returned by function connect_mongo(database)
        A mongodb database object
    collection: string
        The name of a mongodb collection
    Returns
    -------
    out: True or False
        Returns True if the url already exists in the mongodb collection
        Returns False if the url does not exist yet.
    """
    collection = eval('db.%s' % collection)
    urlWithoutHttp = url.split("//")[1]
    cursor = db.Finding.find({'Url': {'$regex': urlWithoutHttp}})
    if len(list(cursor)) == 0:
        return False
    else:
        "Already exists"
        return True


def url_includes_exclusion(url, exclusions):
    """
    Tags a url as to be excluded  or not based on whether it contains
    certain terms
    Parameters
    ----------
    url: string
        A url for example 'https://www.bbc.com/....html'
    exclusions: list
        A list of strings
        For example [u'wikipedia', u'bmi.ir', u'abanka.si']
    Returns
    -------
    out: string
        "Yes" if url includes one of the terms, "No" if it does not contain
        any of the terms.
    Examples
    --------
    >>> url = "https://en.wikipedia.org/wiki/Bank_Melli_Iran"
    >>> exclusions = [u'wikipedia', u'bmi.ir', u'abanka.si']
    >>> url_includes_exclusion(url, exclusions)
    >>> Yes
    """
    exclusion = "No"
    for e in exclusions:
        if str(e).lower() in url.lower():
            exclusion = "Yes"
    return exclusion


def get_headline(html):
    """
    Description
    Creates a headline for findings obtained via google_search(subjects,
                                                               topics).
    (Findings obtained via news_api(subjects, topics),
    already come with a headline.)
    ----------
    html : str
        The source HTML from which to extract the content.
    Returns
    -------
    out : str
        Returns the h1 heading or if not found h2 heading
        and if both not found or empty, "No header found"
    Examples
    --------
    >>> get_headline(
            "<html><body>" \
            <h1>Only good news today</h1>" \
            "<p>Woman Catches Thief Then Takes Him Out For Coffee</p>" \
            "<p>Dog Found Tied to Tree With a Note is Adopted</p>" \
            "<p>Teen Called “Trash Girl” Continues to Save the Planet</p>" \
            "<p>Man Wins By Playing Numbers That Came to Him in a Dream</p>" \
            "</body></html>")
    >>> "Only good news today"
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    if soup.find('h1'):
        h1 = soup.select('h1')[0].text.strip()
    else:
        h1 = ""
    if soup.find('h2'):
        h2 = soup.select('h2')[0].text.strip()
    else:
        h2 = "h2"
    h_no = "No header found"
    if len(h1) > 0:
        h = h1
    elif len(h2) > 0:
        h = h2
    else:
        h = h_no
    return h


def extract_article_content(html, url):
    """
    Disclaimer
    ----------
    Copied from
    https://github.com/turi-code/how-to/blob/master/
            extract_article_content_from_HTML.py
    Description
    ----------
    Extract the primary textual content from an HTML news article.
    In many cases, the HTML source of news articles is littered with
    boilerplate text that you would not want to include when doing text
    analysis on the content the page. Even if you could write some rules to
    extract the content from one page, it's unlikely that those rules would
    apply to an article from another site. The boilerpipe module allows us to
    solve this problem more generally.
    Parameters
    ----------
    html : str
        The source HTML from which to extract the content.
    url : str
        The url, needed for logging purposes only
    Returns
    -------
    out : str
        The primary content of the page with all HTML and boilerplate text
        removed.
    Examples
    --------
    >>> extract_article_content(
            "<html><body><p>Turi is in the business of building the best " \
            "machine learning platform on the planet. Our goal is to make " \
            "it easy for data scientists to build intelligent, predictive " \
            "applications quickly and at scale. Given the perplexing array " \
            "of tools in this space, we often get asked "Why Turi? What " \
            "differentiates it from tools X, Y, and Z?" This blog post aims " \
            "to provide some answers. I’ll go into some technical details " \
            "about the challenges of building a predictive application, and " \
            "how Turi’s ML platform can help.</p></body></html>")
    >>> Turi is in the business of building the best " \
            "machine learning platform on the planet. Our goal is to make " \
            "it easy for data scientists to build intelligent, predictive " \
            "applications quickly and at scale. Given the perplexing array " \
            "of tools in this space, we often get asked "Why Turi? What " \
            "differentiates it from tools X, Y, and Z?" This blog post aims " \
            "to provide some answers. I’ll go into some technical details " \
            "about the challenges of building a predictive application, and " \
            "how Turi’s ML platform can help.
    See Also
    --------
    - `Boilerpipe project <https://code.google.com/p/boilerpipe/>`_
    - `Boilerpipe Python module <https://pypi.python.org/pypi/boilerpipe>`_
    """
    from boilerpipe.extract import Extractor
    if html and html.strip():
        try:
            extractor = Extractor(extractor='ArticleExtractor',
                                  html=html)
            return extractor.getText()
        except Exception as e:
            error = "Function extract_article_content: " + url + " - " + str(e)
            # file = open("/home/www/Ana/AnaDownloadErrorLog.txt", "a")
            # file.write(error)
            # file.close()


def get_url_details(subjects, topics, url):
    """
    Function to examine the html of a particular url in more detail
    We found that quite often a url is returned by the initial search,
    but if we examine the page itself we can actually not find the subject
    or/nor any of the search terms.  If this is the case we do not expect
    the article to be of real interest.  These urls are still stored in
    the database, since we want to evidence and we want to ensure that
    next time the script runs the same url is not examinded again in detail,
    which should speed up the subsequent runs.
    To do:
    - Is there a way to also examine PDF's, do we want to?  They are excluded
      now.
    - We should store the urls which do not load in another mongodb collection
      so the developers can further examine then.  It could be that due to bugs
      in the code, the except route is taken and so this should be monitored.
    Parameters
    ----------
    subjects: list
        A list of companies for which we want to capture the news
    topics: list
        A list of topics we want to search on
    url: str
        The url to be examined in more detail
    Returns
    -------
    out: html, article, headline, subjectsDict, subjectsList, topicsDict,
         topicsList, datesDict
        html: The full html of the finding
        article: result of extract_article_content(html, url)
        headline: result of headline = get_headline(html)
        subjectsDict: a dict with the subjects as keys and the number of time
                      this subject was found in the html as value
                      for example: {u'bank melli': 2}
        subjectsList: same as above, but in the form of a list, for example:
                      [u'bank melli']
        topicsDict: a dict with the topics as keys and the number of time this
                    topic was found in the html as value
                    for example: {u'money laundering': 1, u'sanctions': 6}
        topicsList: same as above, but in the form of a list, for example:
                    ['money laundering', 'sanctions']
        datesDict: the min and the max date of all the dates of format
                   "\d\d\d\d-\d\d-\d\d" which could be found in the html
                   for example: {u'maxDate': 
                       datetime.datetime(2017, 10, 31, 0, 0),
                       u'minDate': datetime.datetime(2017, 10, 30, 0, 0)}
    Examples
    --------
    >>> out: html, article, headline, subjectsDict, subjectsList, topicsDict,
             topicsList, datesDict = get_url_details(['bank melli'], ['fraud'],
             "https://www.reuters.com/article/iran-banks/...")
        html: "<!--[if !IE]> This has been served from cache <![endif]-->
               ...
               </body></html>"
        article: "u'September 27, 2011 /  5:00 PM / 7 years ago\nUPDATE 1-Iran
                  ...
                 (Additional reporting by Hossein Jaseb; Editing by ...'"
        headline: "u'UPDATE 1-Iran bank chiefs ousted in $2.6 bln fraud ...'"
        subjectsDict: {u'bank melli': 8}
        subjectsList: [u'bank melli']
        topicsDict: {u'fraud': 40}
        topicsList: [u'fraud']
        datesDict: {'maxDate': datetime.datetime(2011, 9, 27, 0, 0),
                    'minDate': datetime.datetime(2011, 9, 27, 0, 0)}
    """
    import requests
    import datetime
    import re
    subjects_search = "|".join([s for s in subjects])
    topics_search = "|".join(topics)
    if url[-3:].lower() != 'pdf':
        try:
            responseCheck = requests.get(url)
            html = responseCheck.text
        except Exception as e:
            error = str(e) + " - " + url
            # file = open("AnaDownloadErrorLog.txt", "a")
            # file.write(error)
            # file.close()
            html = "Did not load"
        if html == "Did not load":
            article = "Did not load"
            headline = "Did not load"
            subjectsDict = {}
            subjectsList = []
            topicsDict = {}
            topicsList = []
            datesDict = {}
        else:
            topics_found = re.findall(topics_search, html.lower())
            subjects_found = re.findall(subjects_search, html.lower())
            dates = re.findall("\d\d\d\d-\d\d-\d\d", html)
            topicsDict = {}
            subjectsDict = {}
            datesDict = {}
            for subject in subjects_found:
                if subject in subjectsDict:
                    subjectsDict[subject] += 1
                else:
                    subjectsDict[subject] = 1
            for topic in topics_found:
                if topic in topicsDict:
                    topicsDict[topic] += 1
                else:
                    topicsDict[topic] = 1
            try:
                dates = [datetime.datetime.strptime(d, "%Y-%m-%d")
                         for d in dates]
                datesDict['minDate'] = min(dates)
                datesDict['maxDate'] = max(dates)
            except Exception as e:
                error = str(e) + " - " + str(dates)
                # file = open("AnaDownloadErrorLog.txt", "a")
                # file.write(error)
                # file.close()
            article = extract_article_content(html, url)
            headline = get_headline(html)
        subjectsList = subjectsDict.keys()
        topicsList = topicsDict.keys()
    else:
        error = "This is a pdf " + url
        # file = open("AnaDownloadErrorLog.txt", "a")
        # file.write(error)
        # file.close()
        html = "Did not load"
        article = "Did not load"
        headline = "Did not load"
        subjectsDict = {}
        subjectsList = []
        topicsDict = {}
        topicsList = []
        datesDict = {}
    return html, article, headline, subjectsDict, subjectsList, \
        topicsDict, topicsList, datesDict


def store_doc_in_mongo(doc, db, collection):
    """
    Stores the finding as a record (document) in the database
    (mongodb collection).
    Parameters
    ----------
    doc: dict
         The record (document) to be stored, for example:
         doc = {'Url': url.encode("utf-8"),
                'Source': news_api_results[url]['source'],
                ...
                'Repeated': repeated,
                'ToBeUploaded': to_be_uploaded}
    db: object returned by function connect_mongo(database)
        The mongodb database in which to store the document
    collection: string
        The name of a mongodb collection within the db in which to store the
        document
    """
    collection = eval('db.%s' % collection)
    try:
        collection.insert_one(doc)
    except Exception as e:
        error = str(e)  # Have to add something
        # file = open("AnaDownloadErrorLog.txt", "a")
        # file.write(error)
        # file.close()


def nn_get_data(db, collection):
    """
    Gets the data from the mongo collection on which to apply the nearest
    neighbor model: all articles where there is something in the article and
    which is worth while examining
    Parameters
    ----------
    db: object returned by function connect_mongo(database)
        A mongodb database object
    collection: string
        The name of a mongodb collection
    Returns
    -------
    >>> out: df, articles
        df: a pandas dataframe with all de data
            1  Argument:  ...    2018-01-31 04:58:17.754 ...
        articles: a pandas series with only the articles
    Examples
    --------
    >>> nn_get_data(db, "Finding")
    >>> df: a pandas dataframe with all de data
                        Article  DateDownload  ...
            0  You've been...    2018-01-31 04:58:17.189 ...
            1  Argument:  ...    2018-01-31 04:58:17.754 ...
        articles: a pandas series with only the articles
            877     More\nBy SOUAD MEKHENNET AND JOBY WARRICK  | ...
            878    Purchase: Order Reprint\nMANAMA, Bahrain – Inv...
    """
    import pandas as pd
    collection = eval('db.%s' % collection)
    cursor = collection.find({'$and': [{'Article': {'$exists': True}},
                                       {'Article': {'$ne': ''}},
                                       {'Article': {'$ne': None}},
                                       {'Examined': {'$ne': 'NA'}}]})
    df = pd.DataFrame(list(cursor))
    df['Id'] = df.index
    articles = df['Article']
    return df, articles


def nn_create_model(articles):
    """
    Creates a machine learning nearest neighbor clustering model using the term
    frequency inverse document frequency
    of the words in the articles as features.
    Parameters
    ----------
    articles: pandas Series
        result of function nn_get_data(db, collection)
    Returns
    -------
    out: nn_tfidf_model
        sklearn nearest neighbor model
    """
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.neighbors import NearestNeighbors
    from sklearn.externals import joblib
    tfidf_vectorizer_articles = TfidfVectorizer(decode_error='ignore',
                                                stop_words='english')
    tfidf_articles = tfidf_vectorizer_articles.fit_transform(articles)
    nn_tfidf_model = NearestNeighbors()
    nn_tfidf_model.fit(tfidf_articles)
    joblib.dump(nn_tfidf_model, "nn_tfidf_model.pkl")
    return nn_tfidf_model


def nn_apply_model(model, nn_no):
    """
    Applies the model and creates an array with distances and and array with
    indices for each article for the nn_no nearest neighbors
    Parameters
    ----------
    model: sklearn nearest neighbor model
        Result of function nn_create_model(articles)
    nn_no: integer
        The number of nearest neighbors
    Returns
    -------
    out: dist, ind
        dist: array with distances for each article to its 5 nearest neigbors
        ind: array with indices for each article to its five nearest neigbors
    """
    dist, ind = model.kneighbors(n_neighbors=nn_no)
    return dist, ind


def lr_predict_get_data(db, collection):
    """
    Gets the records from mongo where usefulness has not been predicted
    so we can apply the trained and saved logistic regression classifier on it
    and predict whether the Finding will be deemed useful or not.
    Parameters
    ----------
    db: object returned by function connect_mongo(database)
        A mongodb database object
    collection: string
        The name of a mongodb collection
    Returns
    -------
    >>> out: df, article
        df: a pandas dataframe with the records for which there is no
        prediction yet
        articles: a pandas series with only the data in the Article field
        (same condition as for df)
    Examples
    --------
    >>> nn_get_data(db, "Finding")
    >>> df: a pandas dataframe with the new record
                        Article  DateDownload
            0  You've been...    2018-01-31 04:58:17.189
        articles: a pandas series with only the article
            0  You've been...
    """
    import pandas as pd
    collection = eval('db.%s' % collection)
    cursor = collection.find({'$and': [{'Article': {'$exists': True}},
                                       {'Article': {'$ne': ''}},
                                       {'Article': {'$ne': None}},
                                       {'Examined': {'$ne': 'NA'}},
                                       {'PredictUsefulness':
                                           {'$exists': False}}]})
    df = pd.DataFrame(list(cursor))
    articles = df['Article']
    return df, articles


def lr_predict_create_x_and_y(articles, count_vect):
    """
    Prepares the data for which we want to make a prediction, so it can be used
    in a logistic regression classifier.
    Parameters
    ----------
    articles: a pandas series with only the data in the 'Article' field
        Returned by function lr_predict_get_data(db, collection)
    count_vect: CountVectorizer object (bag of words)
        Stored on disk when model is trained
    Returns
    -------
    out: x_to_predict
        scipy.sparse.csr.csr_matrix
        data for which we want to make a prediction transformed using the same
        CountVectorizer used during training
    Example
    -------
    >>> lr_predict_create_x_and_y(articles, count_vect)
    >>> x_to_predict
        (0, 3648)   1
        (0, 3710)   1
        (0, 3881)   1
    """
    x_to_predict = count_vect.transform(articles)
    return x_to_predict

# %% The main act


def main():

    # What should we do when the previous investigations are not completed?

    start = datetime.datetime.now()

    path = sys.argv[1]
    path_pdfs = path + "static/pdfs/"
    db_name = sys.argv[1].split("/")[-2]  # Should be Ana, Ana1, Ana2
    db = connect_mongo(db_name)

    counter_new = 0
    counter_exclusion = 0
    counter_to_be_examined = 0
    urls_count = 0
    no_of_calls = 0

    investigation_list = get_new_investigations(db, 'Investigation')

    number_of_investigations = len(investigation_list)

    if investigation_list == "Nothing to investigate":
        end = datetime.datetime.now()

        statistics_doc = {'Start': start,
                          'End': end,
                          'NoOfInvestigations': 0,
                          'NoOfRuns': 0,
                          'NoOfUrlsExamined': 0,
                          'NoOfNewUrls': 0,
                          'NoOfUrlsToBeExamined': 0,
                          'NoOfUrlExcluded': 0}
        store_doc_in_mongo(statistics_doc, db, 'Statistics')

        sys.exit()
    else:

        # Change status of collection Run to "Scraping the www"
        change_investigation_status(db,
                                    'Investigation',
                                    investigation_list,
                                    "Scraping the www")
        print "Scraping the www"
        # If one of the investigations is the full daily investigation which
        # runs at 9am
        # We ensure that a new full investigation is scheduled for the next day
        # at 9am
        investigation_requestor_list = [i['investigation_requestor']
                                        for i in investigation_list]
        if "SYSTEM" in investigation_requestor_list:
            tomorrow = datetime.datetime.now().date() + \
                datetime.timedelta(days=1)
            tomorrow_midnight = \
                datetime.datetime.combine(tomorrow,
                                          datetime.datetime.min.time())
            tomorrow_9am = tomorrow_midnight + datetime.timedelta(hours=9)
            # To check if the investigation was already added
            investigation_already_exists = len(list(db.Investigation.find(
                    {"TopicGroup": ["All"],
                     "SubjectGroup": ["All"],
                     "ScheduledDateTime": tomorrow_9am,
                     "Investigator": "SYSTEM"}))) > 0
            if not investigation_already_exists:
                db.Investigation.insert_one({"TopicGroup": ["All"],
                                             "SubjectGroup": ["All"],
                                             "ScheduledDateTime": tomorrow_9am,
                                             "Investigator": "SYSTEM"})

        # Start the different runs to get all the info for the different
        # investigations

        runs = define_runs(investigation_list)
        number_of_runs = len(runs)

        for run in runs:
            subject_group = run
            topic_group = runs[subject_group]
            subjects = get_subjects(db, 'Subject', subject_group)
            topics = get_topics(db, 'Topic', topic_group)

            # exclusions should perhaps also be part of the investigation,
            # just like subject and topic?

            exclusions = exclude_url_including(db)

            news_api_results = {}
            # news_api_results = news_api(subjects, topics, path)
            no_of_calls, news_api_result = news_api_subjects_only(subjects,
                                                                  no_of_calls,
                                                                  path)
            urls_count = urls_count + len(news_api_results)

            for url in news_api_results.keys():
                if not check_url_exists(url, db, 'Finding'):
                    counter_new += 1
                    (html, article, headline, subjectsDict,
                     subjectsList, topicsDict, topicsList, datesDict) = \
                        get_url_details(subjects, topics, url)
                    if subjectsDict == {} or topicsDict == {}:
                        default = "NA"
                        examined = default
                        examined_by = default
                        summary = default
                        useful = default
                        older_than_5_years = default
                        repeated = default
                        to_be_uploaded = default
                    else:
                        default = "To be filled"
                        examined = default
                        examined_by = default
                        summary = default
                        useful = default
                        older_than_5_years = default
                        repeated = default
                        to_be_uploaded = default
                        counter_to_be_examined += 1
                    exclusion = url_includes_exclusion(url, exclusions)
                    if exclusion == "Yes":
                        counter_exclusion += 1
                    now = datetime.datetime.now()
                    now_date = datetime.datetime.today().replace(hour=0,
                                                                 minute=0,
                                                                 second=0,
                                                                 microsecond=0)
                    finding_doc = {'Url': url.encode("utf-8"),
                                   'Source': news_api_results[url]['source'],
                                   'Html': html.encode("utf-8"),
                                   'Article': article,
                                   'Headline':
                                       news_api_results[url]['headline'],
                                   'Subjects': subjectsDict,
                                   'SubjectList': subjectsList,
                                   'Topics': topicsDict,
                                   'TopicList': topicsList,
                                   'DatesHtml':
                                       news_api_results[url]['dateshtml'],
                                   'DateDownload': now,
                                   'DateDownloadDateOnly': now_date,
                                   'Exclusion': exclusion,
                                   'LastModified': now,
                                   'Examined': examined,
                                   'ExaminedBy': examined_by,
                                   'Summary': summary,
                                   'Useful': useful,
                                   'OlderThan5Years': older_than_5_years,
                                   'Repeated': repeated,
                                   'ToBeUploaded': to_be_uploaded}
                    store_doc_in_mongo(finding_doc, db, 'Finding')

                    # Downloading the PDF - this needs to happen after the doc
                    # is stored since the name of the pdf is the objectid
                    last_id = list(db.Finding.find({},
                                   {'_id': 1})
                                   .limit(1)
                                   .sort([('_id',
                                           pymongo.DESCENDING)]))[0]['_id']
                    name_pdf = 'PDF' + str(last_id) + '.pdf'
                    v = path_pdfs + 'wkhtmltopdf.sh' + ' ' + url + ' ' + \
                        path_pdfs + name_pdf
                    os.system(v)
                    db.Finding.update({'_id': ObjectId(last_id)},
                                      {'$set': {"PathToPdf": name_pdf}})

        # Change status of collection Run to "Predicting usefulness using AI"
        change_investigation_status(db,
                                    'Investigation',
                                    investigation_list,
                                    "Predicting usefulness using AI")
        print "Predicting usefulness using AI"
        # Loading the logistic regression model previously trained as well as
        # the bag of words algo used
        model = joblib.load(path + "lr_model.pkl")
        count_vect = joblib.load(path + "lr_vect.pkl")
        df_to_predict, articles_to_predict = lr_predict_get_data(db, "Finding")
        x_to_predict = lr_predict_create_x_and_y(articles_to_predict,
                                                 count_vect)
        predict_proba = model.predict_proba(x_to_predict)
        counter = 0
        for pp in predict_proba:
            predicted_usefulness = round(pp[1] * 100, 4)
            _id = df_to_predict.iloc[counter]['_id']
            counter += 1
            db.Finding.update_one({"_id": _id},
                                  {'$set': {'PredictedUsefulness':
                                            predicted_usefulness}})

        # Change status of collection Run to "Searching nearest neighbors
        # using AI"
        change_investigation_status(db,
                                    'Investigation',
                                    investigation_list,
                                    "Searching nearest neighbors using AI")
        print "Searching nearest neighbors using AI"
        df, articles = nn_get_data(db, "Finding")
        nn_tfidf_model = nn_create_model(articles)
        dist, ind = nn_apply_model(model=nn_tfidf_model, nn_no=5)
        db.Finding_NN.drop()
        for row in ind:
            row_ind = np.where(np.all(ind == row, axis=1))[0][0]
            article_id = df.iloc[row_ind]['_id']
            for element in row:
                col_ind = list(row).index(element)
                neighbor_id = df.iloc[element]['_id']
                distance = dist[row_ind, col_ind]
                doc = {'Main': ObjectId(article_id),
                       'Neighbor': ObjectId(neighbor_id),
                       'Distance': distance}
                store_doc_in_mongo(doc, db, "Finding_NN")

        # Change status of collection Run to "Updating Stats"
        change_investigation_status(db,
                                    'Investigation',
                                    investigation_list,
                                    "Updating Stats")
        print "Updating Stats"
        end = datetime.datetime.now()

        statistics_doc = {'Start': start,
                          'End': end,
                          'NoOfInvestigations': number_of_investigations,
                          'NoOfRuns': number_of_runs,
                          'NoOfCalls': no_of_calls,
                          'NoOfUrlsExamined': urls_count,
                          'NoOfNewUrls': counter_new,
                          'NoOfUrlsToBeExamined': counter_to_be_examined,
                          'NoOfUrlExcluded': counter_exclusion}
        store_doc_in_mongo(statistics_doc, db, 'Statistics')

        # Change status of collection Run to "Investigation Completed"
        change_investigation_status(db,
                                    'Investigation',
                                    investigation_list,
                                    "Investigation Completed")
        print "Investigation Completed"

# %% Standard boilerplate to call the main() function


if __name__ == "__main__":
    main()
