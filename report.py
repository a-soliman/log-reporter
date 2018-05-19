import psycopg2
import datetime

DBNAME = "news"

def get_slugs():
    db      = psycopg2.connect(database=DBNAME)
    cursor  = db.cursor()
    query   = "SELECT slug FROM articles"
    cursor.execute(query)
    slugs = [slug[0] for slug in cursor.fetchall()]
    db.close()
    return slugs

def get_authors():
    db      = psycopg2.connect(database=DBNAME)
    cursor  = db.cursor()
    query   = "SELECT name FROM authors"
    cursor.execute(query)
    authors = [author[0] for author in cursor.fetchall()]
    db.close()
    return authors


def test_log():
    db      = psycopg2.connect(database=DBNAME)
    cursor  = db.cursor()
    query   = "SELECT * FROM log"
    cursor.execute(query)
    result  = [log[0] for log in cursor.fetchall()]
    db.close()
    return result

# print(get_slugs())

def get_most_popular_articles():
    db      = psycopg2.connect(database=DBNAME)
    cursor  = db.cursor()
    query   = """SELECT 
        articles.title, COUNT(log.path) AS views 
        FROM log INNER JOIN articles 
        ON replace(log.path, '/article/', '') = articles.slug 
        WHERE log.status LIKE '%OK%' 
        GROUP BY title 
        ORDER BY views DESC LIMIT 3;
        """
    cursor.execute(query)
    result  = [{"title": article[0], "views": int(article[1])} for article in cursor.fetchall()]
    db.close()
    return result

print(get_most_popular_articles())

def get_most_popular_authors():
    db      = psycopg2.connect(database=DBNAME)
    cursor  = db.cursor()
    query   ="""SELECT 
        authors.name, COUNT(log.path) AS views 
        FROM log 
        INNER JOIN articles 
        ON replace(log.path, '/article/', '') = articles.slug 
        inner join authors 
        ON authors.id = articles.author 
        WHERE log.status LIKE '%OK%' AND log.path != '/' 
        GROUP BY name 
        ORDER BY views DESC;
        """
    cursor.execute(query)
    result = [{"name": author[0], "views": int(author[1])} for author in cursor.fetchall()]
    db.close()
    return result

print(get_most_popular_authors())
