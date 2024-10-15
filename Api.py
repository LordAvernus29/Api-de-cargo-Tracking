from flask import Flask, request, Response
from pymongo import MongoClient
import requests
from bson import json_util  # Usamos json_util para serialización correcta
from scraper import login, extract_accounts_data, extract_invoice_data, extract_store_data, save_data_to_mongodb

app = Flask(__name__)

# Configurar MongoDB con Atlas URI sin validar certificados SSL
client = MongoClient("mongodb+srv://luizshernandez6:KX67Prz7xgjAhUko@cluster0.5nc4g.mongodb.net/?retryWrites=true&w=majority&tlsAllowInvalidCertificates=true")
db = client["cargo_track_data"]

@app.route("/accounts", methods=["GET"])
def get_accounts():
    accounts = list(db["accounts"].find({}))  # Obtener todos los documentos
    return Response(
        json_util.dumps(accounts, indent=4),  # Usamos indent=4 para organizar el JSON
        mimetype='application/json'
    )

@app.route("/invoices", methods=["GET"])
def get_invoices():
    invoices = list(db["invoices"].find({}))  # Obtener todos los documentos
    return Response(
        json_util.dumps(invoices, indent=4),  # Usamos indent=4 para organizar el JSON
        mimetype='application/json'
    )

@app.route("/stores", methods=["GET"])
def get_stores():
    stores = list(db["stores"].find({}))  # Obtener todos los documentos
    return Response(
        json_util.dumps(stores, indent=4),  # Usamos indent=4 para organizar el JSON
        mimetype='application/json'
    )

@app.route("/track/<string:tracking_id>", methods=["GET"])
def track_package(tracking_id):
    package = db["stores"].find_one({"Número": tracking_id})
    if package:
        return Response(
            json_util.dumps(package, indent=4),  # Usamos indent=4 para organizar el JSON
            mimetype='application/json'
        )
    else:
        return Response(
            json_util.dumps({"error": "Paquete no encontrado"}, indent=4),
            mimetype='application/json',
            status=404
        )

# Ruta para ejecutar el scraper y actualizar los datos en MongoDB
@app.route("/scrape", methods=["GET"])
def run_scraper():
    try:
        session = requests.Session()
        user = "VE30940"
        password = "f16cargo"
        
        # Iniciar sesión usando la función login desde el scraper
        login(user, password, session)

        # Ejecutar el scraper para extraer datos
        accounts_data = extract_accounts_data(session)
        invoice_data = extract_invoice_data(session)
        store_data = extract_store_data(session)

        # Guardar los datos en MongoDB
        save_data_to_mongodb("accounts", accounts_data)
        save_data_to_mongodb("invoices", invoice_data)
        save_data_to_mongodb("stores", store_data)
        
        return Response(
            json_util.dumps({"message": "Scraper ejecutado y datos actualizados"}, indent=4),
            mimetype='application/json',
            status=200
        )
    except RecursionError as e:
        return Response(
            json_util.dumps({"error": "Error de recursión: " + str(e)}, indent=4),
            mimetype='application/json',
            status=500
        )

if __name__ == "__main__":
    app.run(debug=True)
