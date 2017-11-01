#!/usr/bin/env python3
#
#

import psycopg2


def create_view_toparticles(db):
    """Crete view on init

    Creates a mandatory view for the queries if script is invoked directly.
    As no 'if not exists' statement exists for views in PostgreSQL we do
    try to create the view and catch the error if it exists already.
    """
    c_sql = ("CREATE VIEW top_articles AS SELECT a.title, a.author, \
             count(l.path) AS views FROM articles a LEFT JOIN log l ON \
             l.path = '/article/' || a.slug GROUP BY a.title, a.author \
             ORDER BY views DESC;")
    with psycopg2.connect("dbname=" + db) as conn:
        with conn.cursor() as c:
            try:
                c.execute(c_sql)
            except psycopg2.Error, e:  # catches 'relation ... already exists'
                print(e)
    conn.close()


def get_most_popular_articles(db):
    """ Get the top three articles

    Returns a list with the top three articles with title of the article and
    number of views.
    """
    c_sql = ("SELECT title, views FROM top_articles ORDER BY views DESC \
        LIMIT 3;")
    result = None
    with psycopg2.connect("dbname=" + db) as conn:
        with conn.cursor() as c:
            try:
                c.execute(c_sql)
                result = c.fetchall()
            except psycopg2.Error, e:
                print(e)
    conn.close()
    return(result)


def get_most_popular_authors(db):
    """ Get the top authors

    Returns a list with the top authors with name of the author and
    number of views of her/his articles.
    """
    c_sql = ("SELECT au.name, sum(t.views) AS author_views FROM \
        top_articles t JOIN authors au ON au.id = t.author GROUP \
        BY t.author, au.name ORDER BY author_views DESC;")
    result = None
    with psycopg2.connect("dbname=" + db) as conn:
        with conn.cursor() as c:
            try:
                c.execute(c_sql)
                result = c.fetchall()
            except psycopg2.Error, e:
                print(e)
    conn.close()
    return(result)


def get_days_high_error_rate(db, threshold):
    """ Get a list of days where HTTP 404 error rate higher threshold

    Args:
         db: PostgreSQL database name
         threshold: threshold of error rate (float)
    """
    c_sql = ("WITH q1 AS (SELECT log_date, sum(num) AS s_events, max(CASE WHEN\
        (status = '200 OK') THEN num END) AS s_200, max(CASE WHEN (status = \
        '404 NOT FOUND') THEN num END) AS s_404 FROM (SELECT \
        date_trunc('day', time) AS log_date, status, count(status) AS num \
        FROM log GROUP BY date_trunc('day', time), status) AS sq GROUP BY \
        sq.log_date) SELECT log_date, s_404 / s_events AS error_rate FROM \
        q1 WHERE s_404 / s_events > (%s)")
    result = None
    with psycopg2.connect("dbname=" + db) as conn:
        with conn.cursor() as c:
            try:
                c.execute(c_sql, (threshold,))
                result = c.fetchall()
            except psycopg2.Error, e:
                print(e)
    conn.close()
    return(result)


if __name__ == '__main__':
    # put DB name into a variable in case of changess
    db_name = 'news'
    # first, create the necessary view in case it does not exist yet
    create_view_toparticles(db_name)
    # first the top articles - get the result from the db
    top_articles = get_most_popular_articles(db_name)
    # make a (pretty) header
    print("The three most popular articles of all time:")
    print("============================================")
    # go through the result set and make a pretty list with formatted
    # numbers (incl. thousands separator)
    for article in top_articles:
        print("* \"" + article[0] + "\" -- " + '{0:,}'.format(article[1]) +
              " views")
    # add a blank line for better readibility
    print("  ")
    # second the top authors - get the result from the db
    top_authors = get_most_popular_authors(db_name)
    # make a pretty header
    print("The most popular article authors of all time:")
    print("=============================================")
    # go through the result set and make a pretty list with formatted
    # numbers (incl. thousands separator)
    for author in top_authors:
        print("* " + author[0] + " -- " + '{0:,}'.format(author[1]) +
              " views")
    # add a blank line for better readibility
    print("  ")
    # third the days with 404 error rate above 1% - get the data and pass the
    # 1% threshold
    high_error_days = get_days_high_error_rate(db_name, 0.01)
    # make a pretty header
    print("Days on which more than 1% of requests lead to errors:")
    print("======================================================")
    # go through the result set and make a pretty list with formatted
    # values (date as mm/dd/yyyy, error rate in % with 2 digits)
    for e_day in high_error_days:
        print("* " + (e_day[0]).strftime('%m/%d/%Y') + " -- " +
              '{:.2%}'.format(e_day[1]) + " views")
    print("  ")
