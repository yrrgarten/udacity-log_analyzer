## Log Analysis Project

# About

This script outputs the
 * most three popular articles of all time
 * the most popular authors of all time
 * days where more than 1% of all requests lead to errors

from the 'newsdata' SQL provided by Udacity for their Full Stack Developer
Nanodegree.

The actual database can be obtained from Udacity Full Stack Developer course
as a Vagrant image (Ubuntu with PostgreSQL server). You need to be enrolled in
the course. The database content can be obtained through the project page on
the lesson. Follow the instruction provided in the course material to set it
up.

The script connects to the db-server from localhost using psycopg2 as DB-API.
The logical db name is 'news' and can be changed in the script if necessary.


# Usage

It is a Python3 program which simply runs from the command line:

    $ python log_analyzer.py

The script makes use of a SQL view which will be created when the script is
invoked:

    CREATE VIEW
        top_articles
    AS SELECT
        a.title,
        a.author,
        count(l.path) AS views
    FROM
        articles a
    LEFT JOIN
        log l ON l.path = '/article/' || a.slug
    GROUP BY
        a.title,
        a.author
    ORDER BY
        views DESC


# To-Do / Issues

 * function to create view is only called when the script in invoked directly
 * SQL to get the days with a certain error rate is not optimal in regards
 to performance. There is probably a better way to achieve the result...
