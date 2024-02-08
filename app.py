from flask import Flask, jsonify, request
from flask_cors import CORS
import mysql.connector

app = Flask(__name__)
CORS(app)

mysql_config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'p1tnglhe',
    'database': 'sakila'
}

@app.route('/test')
def top_rented_films():
    try:
        db_connection = mysql.connector.connect(**mysql_config)
        cursor = db_connection.cursor()

        query = """
        SELECT film.film_id, film.title, category.name AS category_name, COUNT(rental.rental_id) AS rented
        FROM film
        JOIN inventory ON film.film_id = inventory.film_id
        JOIN rental ON inventory.inventory_id = rental.inventory_id
        JOIN film_category ON film.film_id = film_category.film_id
        JOIN category ON film_category.category_id = category.category_id
        GROUP BY film.film_id, film.title, category.name
        ORDER BY rented DESC
        LIMIT 5
        """
        cursor.execute(query)
        top_rented_films = cursor.fetchall()

        films_data = []
        for row in top_rented_films:
            film_data = {
                'film_id': row[0],
                'title': row[1],
                'category_name': row[2],
                'rented': row[3]
            }
            films_data.append(film_data)

        return jsonify(films_data)
    except Exception as e:
        print("Error fetching top rented films:", e)
        return jsonify({'error': 'Error fetching top rented films'})
    finally:
        if 'db_connection' in locals() and db_connection.is_connected():
            cursor.close()
            db_connection.close()

@app.route('/rest')
def top_actors():
    try:
        db_connection = mysql.connector.connect(**mysql_config)
        cursor = db_connection.cursor()

        query = """
        SELECT actor.actor_id, actor.first_name, actor.last_name, COUNT(film_actor.film_id) AS movies
        FROM actor
        JOIN film_actor ON actor.actor_id = film_actor.actor_id
        GROUP BY actor.actor_id, actor.first_name, actor.last_name
        ORDER BY movies DESC
        LIMIT 5
        """
        cursor.execute(query)
        top_actors_data = cursor.fetchall()

        actors_data = []
        for row in top_actors_data:
            actor_data = {
                'actor_id': row[0],
                'first_name': row[1],
                'last_name': row[2],
                'movies': row[3]
            }
            actors_data.append(actor_data)

        return jsonify(actors_data)
    except Exception as e:
        print("Error fetching top actors:", e)
        return jsonify({'error': 'Error fetching top actors'})
    finally:
        if 'db_connection' in locals() and db_connection.is_connected():
            cursor.close()
            db_connection.close()

@app.route('/actor-films/<int:actor_id>')
def actor_films(actor_id):
    try:
        db_connection = mysql.connector.connect(**mysql_config)
        cursor = db_connection.cursor()

        query = """
        SELECT film.film_id, film.title, COUNT(rental.rental_id) AS rental_count
        FROM film
        JOIN film_actor ON film.film_id = film_actor.film_id
        JOIN actor ON film_actor.actor_id = actor.actor_id
        JOIN inventory ON film.film_id = inventory.film_id
        JOIN rental ON inventory.inventory_id = rental.inventory_id
        WHERE actor.actor_id = %s
        GROUP BY film.film_id, film.title
        ORDER BY rental_count DESC
        LIMIT 5
        """
        cursor.execute(query, (actor_id,))
        top_actor_films = cursor.fetchall()

        films_data = []
        for row in top_actor_films:
            film_data = {
                'film_id': row[0],
                'title': row[1],
                'rental_count': row[2]
            }
            films_data.append(film_data)

        return jsonify(films_data)
    except Exception as e:
        print("Error fetching top actor films:", e)
        return jsonify({'error': 'Error fetching top actor films'})
    finally:
        if 'db_connection' in locals() and db_connection.is_connected():
            cursor.close()
            db_connection.close()

@app.route('/search-films')
def search_films():
    try:
        db_connection = mysql.connector.connect(**mysql_config)
        cursor = db_connection.cursor()

        film_name = request.args.get('film_name')

        query = """
        SELECT film.film_id, film.title, film.description, category.name AS category_name, film.release_year
        FROM film
        JOIN film_category ON film.film_id = film_category.film_id
        JOIN category ON film_category.category_id = category.category_id
        WHERE film.title LIKE %s
        """

        cursor.execute(query, (f'%{film_name}%',))
        search_results = cursor.fetchall()

        films_data = []
        for row in search_results:
            film_data = {
                'film_id': row[0],
                'title': row[1],
                'description': row[2],
                'category_name': row[3],
                'release_year': row[4]
            }
            films_data.append(film_data)

        return jsonify(films_data)
    except Exception as e:
        print("Error searching films:", e)
        return jsonify({'error': 'Error searching films'})
    finally:
        if 'db_connection' in locals() and db_connection.is_connected():
            cursor.close()
            db_connection.close()

@app.route('/search-films-by-actor')
def search_films_by_actor():
    try:
        db_connection = mysql.connector.connect(**mysql_config)
        cursor = db_connection.cursor()

        actor_name = request.args.get('actor_name')

        query = """
        SELECT film.film_id, film.title, film.description, category.name AS category_name, film.release_year
        FROM film
        JOIN film_actor ON film.film_id = film_actor.film_id
        JOIN actor ON film_actor.actor_id = actor.actor_id
        JOIN film_category ON film.film_id = film_category.film_id
        JOIN category ON film_category.category_id = category.category_id
        WHERE actor.first_name LIKE %s OR actor.last_name LIKE %s
        """

        cursor.execute(query, (f'%{actor_name}%', f'%{actor_name}%'))
        search_results = cursor.fetchall()

        films_data = []
        for row in search_results:
            film_data = {
                'film_id': row[0],
                'title': row[1],
                'description': row[2],
                'category_name': row[3],
                'release_year': row[4]
            }
            films_data.append(film_data)

        return jsonify(films_data)
    except Exception as e:
        print("Error searching films by actor:", e)
        return jsonify({'error': 'Error searching films by actor'})
    finally:
        if 'db_connection' in locals() and db_connection.is_connected():
            cursor.close()
            db_connection.close()

@app.route('/search-films-by-genre')
def search_films_by_genre():
    try:
        db_connection = mysql.connector.connect(**mysql_config)
        cursor = db_connection.cursor()

        genre = request.args.get('genre')

        query = """
        SELECT film.film_id, film.title, film.description, category.name AS category_name, film.release_year
        FROM film
        JOIN film_category ON film.film_id = film_category.film_id
        JOIN category ON film_category.category_id = category.category_id
        WHERE category.name LIKE %s
        """
        
        cursor.execute(query, (f'%{genre}%',))
        search_results = cursor.fetchall()

        films_data = []
        for row in search_results:
            film_data = {
                'film_id': row[0],
                'title': row[1],
                'description': row[2],
                'category_name': row[3],
                'release_year': row[4]
            }
            films_data.append(film_data)

        return jsonify(films_data)
    except Exception as e:
        print("Error searching films by genre:", e)
        return jsonify({'error': 'Error searching films by genre'})
    finally:
        if 'db_connection' in locals() and db_connection.is_connected():
            cursor.close()
            db_connection.close()

@app.route('/customers')
def get_customers():
    try:
        db_connection = mysql.connector.connect(**mysql_config)
        cursor = db_connection.cursor()

        query = """
        SELECT customer_id, first_name, last_name FROM customer
        """
        cursor.execute(query)
        customers = cursor.fetchall()

        customers_data = []
        for row in customers:
            customer_data = {
                'id': row[0],
                'first_name': row[1],
                'last_name': row[2]
            }
            customers_data.append(customer_data)

        return jsonify(customers_data)
    except Exception as e:
        print("Error fetching customers:", e)
        return jsonify({'error': 'Error fetching customers'})
    finally:
        if 'db_connection' in locals() and db_connection.is_connected():
            cursor.close()
            db_connection.close()

if __name__ == '__main__':
    app.run(debug=True)
