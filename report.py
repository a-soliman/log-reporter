import psycopg2

DBNAME = "news"


def get_most_popular_articles():
    '''
    returns a list of the 3 most popular articles,an the views for each.
    '''
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
    result  = [(article[0], int(article[1])) for article in cursor.fetchall()]
    db.close()
    return result


def get_most_popular_authors():
    '''
    returns a list of the most popualr authors and the total views per each.
    '''
    db      = psycopg2.connect(database=DBNAME)
    cursor  = db.cursor()
    query   = """SELECT
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
    result = [(author[0], int(author[1])) for author in cursor.fetchall()]
    db.close()
    return result


def get_error_per_day():
    '''
    returns a list of the days where over 1% error rate occurred, and the % per each.
    '''
    db      = psycopg2.connect(database=DBNAME)
    cursor  = db.cursor()
    query   = """SELECT
        day, ((err + .0) / (total+ .0)) * 100 AS per
        FROM (
            SELECT
                date(time) AS day,
                COUNT(time) AS total,
                SUM(CASE WHEN status ='200 OK' THEN 1 ELSE 0 END) as ok,
                SUM(CASE WHEN status = '404 NOT FOUND' THEN 1 ELSE 0 END) AS err
            FROM log
            GROUP BY day)
            AS tab
            WHERE ((err+.0) / (total+.0)) *100 > 1
            ORDER BY per DESC;
        """
    cursor.execute(query)
    result  = [(str(date[0]), round(float(date[1]), 2)) for date in cursor.fetchall()]
    db.close()
    return result


def report():
    '''
    Prints the opener and closer lines and calls the printer function.
    '''
    print('='*20 + ' REPORT START ' + '='*20)
    printer('articles', get_most_popular_articles())
    printer('authors', get_most_popular_authors())
    printer('error', get_error_per_day())
    print('='*20 + ' REPORT END ' + '='*20)


def printer(_type, data_array):
    '''
    Prints the data to the console.
    Args: 
        1. _type: str.
        2. data_array: a list of tuples.
    '''
    title   = ''
    dash    = ' :==> '
    ending  = ''

    if _type == 'articles':
        title   = 'What are the most popular three articles of all time?'
        ending  = ' views.'

    elif _type == 'authors':
        title   = 'Who are the most popular article authors of all time?'
        ending  = ' views.'

    else:
        title   = 'On which days did more than 1% of requests lead to errors?'
        ending  = '% errors.'

    print('--'*10 + '\n\n** ' + title + '\n\n')

    for line in data_array:
        print(line[0] + dash + str(line[1]) + ending + '\n')
    print('--'*10)

report()
