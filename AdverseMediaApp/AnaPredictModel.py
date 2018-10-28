#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  25 18:32:24 2018
@author: GDP
"""

# %% Import libs used in main

import sys
sys.path.append(r'/home/www/Ana')
from sklearn.externals import joblib
import AnaDownloadFull as ad

# %% Define functions

def lr_train_get_data(db, collection):
    """
    Gets the data from mongo which can be used to train the logistic regression model, aka all the Findings
    which have already been labeled as useful or not, since this is a supervised machine learning problem
    Parameters
    ----------
    db: object returned by function connect_mongo(database)
        A mongodb database object
    collection: string
        The name of a mongodb collection
    Returns
    -------
    >>> out: df, articles
        df: a pandas dataframe with all the labeled data
        articles: a pandas series with only the data in the 'Article' field
    Examples
    --------
    >>> nn_get_data(db, "Finding")
    >>> df: a pandas dataframe with all de data
                        Article  DateDownload            ... Useful
            0  You've been...    2018-01-31 04:58:17.189 ... Yes
            1  Argument:  ...    2018-01-31 04:58:17.754 ... No
        articles: a pandas series with only the articles
            0  You've been...
            1  Argument:  ...
    """
    import pandas as pd
    collection = eval('db.%s' % collection)
    cursor = collection.find({'$and': [{'Article': {'$exists': True}},
                                       {'Article': {'$ne': ''}},
                                       {'Article': {'$ne': None}},
                                       {'Examined': 'Yes'},
                                      ]})
    df = pd.DataFrame(list(cursor))
    articles = df['Article']
    return df, articles


def lr_train_create_x_and_y(df, articles):
    """
    Prepares the data obtained for training purposes so it can be used in a logistic regression classifier.
    Basically we need two matrices/series, one with the input x and one with the output y (aka the true label)
    For the input x we use a bag of words algo, which contains a count of the words in the labeled articles.
    For the output y we use the field 'Useful' and assign the value 0 to articles which were not considered useful
    and 1 for those which were considered useful.
    Parameters
    ----------
    df: a pandas dataframe with all the labeled data
    articles: a pandas series with only the data in the 'Article' field
    both are returned by function lr_train_get_data(db, collection)
    Returns
    -------
    out: count_vect, x_train, y_train
        count_vetc: sklearn CountVectorizer
            The bag of words object
        x_train: scipy.sparse.csr.csr_matrix
            The input to the ml model, the x values, in this case bag of words
        y_train: pandas.core.series.Series
            The y values of the model.  These are the values the model will try to predict as well as possible.
    Example
    -------
    >>> lr_train_create_x_and_y(df, articles)
    >>> count_vect, x_train, y_train
        count_vect
            CountVectorizer(analyzer=u'word', binary=False, decode_error=u'strict',
            dtype=<type 'numpy.int64'>, encoding=u'utf-8', input=u'content',
            lowercase=True, max_df=1.0, max_features=None, min_df=1,
            ngram_range=(1, 1), preprocessor=None, stop_words='english',
            strip_accents=None, token_pattern=u'(?u)\\b\\w\\w+\\b',
            tokenizer=None, vocabulary=None)
        x_train
            (0, 3650)   1
            (0, 3712)   1
            (0, 3884)   1
            ...
        y_train
            0      0
            1      0
            2      0
            ...
    Other
    --------
    http://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.CountVectorizer.html
    """
    from sklearn.feature_extraction.text import CountVectorizer
    count_vect = CountVectorizer(stop_words='english')
    x_train = count_vect.fit_transform(articles)
    df['MLLabel'] = df['Useful'].apply(lambda x: 1 if x == 'Yes' else 0)
    y_train = df['MLLabel']
    return count_vect, x_train, y_train


def lr_train_model(x_train, y_train):
    """
    Creates the machine learning logistic regression classification model
    Parameters
    ----------
    x_train: scipy.sparse.csr.csr_matrix
        The input to the ml model, the x values, in this case bag of words
    y_train: pandas.core.series.Series
        The y values of the model.  These are the values the model will try to predict as well as possible.
    Outputs of lr_train_create_x_and_y(df, articles)
    Returns
    -------
    out: model, confusion_matrix
        model: the ml model
            LogisticRegression(C=1.0, class_weight='balanced', dual=False,
            fit_intercept=True, intercept_scaling=1, max_iter=100,
            multi_class='ovr', n_jobs=1, penalty='l2', random_state=None,
            solver='liblinear', tol=0.0001, verbose=0, warm_start=False)
        confusion_matrix: the accuracy matrix  
            The count of    true negatives is C_{0,0}, 
                            false negatives is C_{1,0}, 
                            true positives is C_{1,1} and 
                            false positives is C_{0,1}
            example:
                array([[275,   0],
               [  0,  15]], dtype=int64)
    Other
    --------
    http://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LogisticRegression.html
    """
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import confusion_matrix
    model = LogisticRegression(class_weight='balanced')
    model.fit(x_train, y_train)
    predict = model.predict(x_train)
    confusion_matrix = confusion_matrix(y_train, predict)
    return model, confusion_matrix

# %% The main act

def main():
    path = '/home/www/Ana/'
    db = ad.connect_mongo("Ana")
    df, articles = lr_train_get_data(db, 'Finding')
    count_vect, x_train, y_train = lr_train_create_x_and_y(df, articles)
    joblib.dump(count_vect, path + "lr_vect.pkl")
    model, confusion_matrix = lr_train_model(x_train, y_train)
    joblib.dump(model, path + "lr_model.pkl")
    joblib.dump(confusion_matrix, path + "lr_cm.pkl")


# %% Standard boilerplate to call the main() function


if __name__ == "__main__":
    main()