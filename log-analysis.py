# Logs Analysis Project.
# DB code for news DB.
# Python version 3.7.0
# Author: Fatima Alsaleh

import psycopg2


def popular_articles():
    db = psycopg2.connect("dbname=news")

    c = db.cursor()
    c.execute("""
    SELECT articles.title, count(*) AS num
    FROM
        log
    JOIN
        articles
    ON articles.slug = substring(log.path,10)
    GROUP BY articles.title
    ORDER BY num DESC LIMIT 3""")
    result = c.fetchall()
    db.close()
    return result


def popular_authors():
    db = psycopg2.connect("dbname=news")

    c = db.cursor()
    c.execute("""
    SELECT authors.name, sum(articleviews.c) AS authorviews
    FROM
        authors,
        (SELECT articles.slug, articles.author, count(*) AS c
            FROM articles,log
            WHERE articles.slug = substring(log.path,10)
            GROUP BY articles.slug, articles.author) AS articleviews
    WHERE authors.id = articleviews.author
    GROUP BY authors.name
    ORDER BY authorviews DESC;""")
    result = c.fetchall()
    db.close()
    return result


def most_errors():
    db = psycopg2.connect("dbname=news")

    c = db.cursor()
    c.execute("""
    SELECT errorstable.date,
    (cast(errorstable.count AS FLOAT)
    /cast(sumtable.total AS FLOAT))*100 percentage
    FROM
        (SELECT Replace(
         To_Char(date(log.time), 'MonthDD,YYYY'),
         '     ', ' ') AS date,count(*) AS count
            FROM log
            WHERE status LIKE '404%'
            GROUP BY date(log.time)) errorstable
    JOIN
        (SELECT Replace(
        To_Char(date(log.time), 'MonthDD,YYYY'),
        '     ', ' ') AS date,count(*) AS total
            FROM log
            GROUP BY date(log.time)) sumtable
    ON errorstable.date = sumtable.date
    WHERE ((cast(errorstable.count AS FLOAT)
          /cast(sumtable.total AS FLOAT))*100) > 1
    ORDER BY percentage DESC;""")
    result = c.fetchall()
    db.close()
    return result


if __name__ == '__main__':

    articles = popular_articles()
    for a in articles:
        print("\"%s\" — %d views" % (a[0], a[1]))

    print(" ")

    authors = popular_authors()
    for w in authors:
        print("%s — %d views" % (w[0], w[1]))

    print(" ")

    errors = most_errors()
    for e in errors:
        print("%s — %.2f%% errors" % (e[0], e[1]))
