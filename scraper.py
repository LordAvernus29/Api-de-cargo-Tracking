import requests
from bs4 import BeautifulSoup
from typing import Any, Dict, List
from pymongo import MongoClient, UpdateOne
from datetime import datetime

base_url = "https://bva.cargotrack.net/default.asp"

def login(user: str, password: str, session: requests.Session) -> None:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "Origin": "https://bva.cargotrack.net",
    }
    data = {"user": user, "password": password, "Submit": "Log In", "action": "login"}
    
    response = session.post(url=base_url, data=data, headers=headers, allow_redirects=True, timeout=10)
    
    if response.status_code != 200:
        raise Exception(f"Error en el login: {response.status_code}")

def extract_data(session: requests.Session, url: str, columns: List[str], table_index: int = 1) -> List[Dict[str, Any]]:
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()  # Asegura que la solicitud fue exitosa
    except requests.exceptions.RequestException as e:
        print(f"Error accediendo a la URL {url}: {str(e)}")
        return []
    
    data: List[Dict[str, Any]] = []
    soup = BeautifulSoup(response.content, "lxml")
    
    # Verificar si existe el div#search
    search_div = soup.select_one("div#search")
    if not search_div:
        print(f"No se encontró el div#search en la página {url}")
        return []
    
    # Intentamos encontrar la tabla
    table = search_div.find_next_sibling("table", table_index)
    if not table:
        print(f"No se encontró la tabla en la página {url}")
        return []
    
    rows = table.find_all("tr")
    for row_number, row in enumerate(rows, start=0):
        row_data = {}
        if row_number == 0:  # Ignoramos la primera fila (encabezado)
            continue
        columns_tds = row.find_all("td")
        for name, value in zip(columns, columns_tds):
            row_data[name] = value.text.strip()
        data.append(row_data)
    return data

def extract_accounts_data(session: requests.Session) -> List[Dict[str, Any]]:
    accounts_url = "https://bva.cargotrack.net/appl2.0/agent/accounts.asp"
    columns = ["Número", "Empresa", "Teléfono", "Móvil", "Email"]
    return extract_data(session, accounts_url, columns)

def extract_invoice_data(session: requests.Session) -> List[Dict[str, Any]]:
    invoices_url = "https://bva.cargotrack.net/appl2.0/agent/invoices.asp"
    columns = ["Fecha", "Numero", "cuenta", "Cantidad", "Pagado"]
    return extract_data(session, invoices_url, columns)

def extract_store_data(session: requests.Session) -> List[Dict[str, Any]]:
    stores_url = "https://bva.cargotrack.net/appl2.0/agent/whs.asp"
    columns = ["Estatus", "Dest", "Almacen", "Fecha", "Remitente", "Destinatario", "Bultos", "Peso", "Volumen", "Valor"]
    return extract_data(session, stores_url, columns)

# Limitar el historial a un número máximo para evitar crecimiento excesivo
MAX_HISTORY_LENGTH = 10

def save_data_to_mongodb(collection_name: str, data: List[Dict[str, Any]]) -> None:
    client = MongoClient("mongodb+srv://luizshernandez6:KX67Prz7xgjAhUko@cluster0.5nc4g.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    db = client["cargo_track_data"]
    collection = db[collection_name]
    
    operations = []
    
    for record in data:
        existing_record = collection.find_one({"Número": record["Número"]})
        
        if existing_record:
            record["created_at"] = existing_record.get("created_at", datetime.utcnow())
            record["updated_at"] = datetime.utcnow()
            record["history"] = existing_record.get("history", [])
            
            # Añadir el historial de cambios pero limitamos el tamaño máximo
            record["history"].append({
                "data": existing_record,
                "changed_at": datetime.utcnow()
            })
            if len(record["history"]) > MAX_HISTORY_LENGTH:
                record["history"] = record["history"][-MAX_HISTORY_LENGTH:]  # Mantener solo los últimos 10 eventos
        else:
            record["created_at"] = datetime.utcnow()
            record["updated_at"] = datetime.utcnow()
            record["history"] = []

        operations.append(UpdateOne(
            {"Número": record["Número"]},
            {"$set": record},
            upsert=True
        ))

    if operations:
        collection.bulk_write(operations)
        print(f"{len(operations)} documentos insertados/actualizados con historial en la colección {collection_name}")
    else:
        print(f"No hay datos para insertar/actualizar en la colección {collection_name}")

# Si ejecutamos el scraper directamente
if __name__ == "__main__":
    user = "VE30940"
    password = "f16cargo"
    session = requests.Session()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "Origin": "https://bva.cargotrack.net",
    }
    session.headers.update(headers)

    home_response = session.get(base_url, timeout=10)
    cookie_value = f"user={user};{home_response.headers['Set-Cookie']}"
    session.headers.update({"cookie": cookie_value})

    try:
        login(user, password, session)
        accounts_data = extract_accounts_data(session)
        invoice_data = extract_invoice_data(session)
        store_data = extract_store_data(session)

        save_data_to_mongodb("accounts", accounts_data)
        save_data_to_mongodb("invoices", invoice_data)
        save_data_to_mongodb("stores", store_data)
    except Exception as e:
        print(f"Error durante la ejecución del scraper: {str(e)}")
