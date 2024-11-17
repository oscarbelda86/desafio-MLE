import requests
import pandas as pd
import csv



# Cargar el archivo CSV
input_file = "claims_dataset.csv"  # Archivo de entrada
output_file = "predictions.csv"    # Archivo de salida

# Leer el archivo de siniestros
data = pd.read_csv(input_file, sep="|")

# Crear archivo CSV de salida con encabezados
with open(output_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["claim_id", "prediction"])  # Columnas de salida

# URL del endpoint de la API
api_url = "http://127.0.0.1:8000/predict"

# Procesar cada fila de forma individual
for _, row in data.iterrows():
    # Crear el diccionario con los datos del siniestro
    siniestro_data = {
        "claim_id": int(row['claim_id']),
        "marca_vehiculo": str(row['marca_vehiculo']),
        "antiguedad_vehiculo": int(row['antiguedad_vehiculo']),
        "tipo_poliza": int(row['tipo_poliza']),
        "taller": int(row['taller']),
        "partes_a_reparar": int(row['partes_a_reparar']),
        "partes_a_reemplazar": int(row['partes_a_reemplazar'])
    }
    siniestro_data = pd.DataFrame([siniestro_data])

    siniestro_data = siniestro_data.to_dict(orient='records')
    # Enviar la consulta a la API
    response = requests.post(api_url, json=siniestro_data[0])
    print(response.status_code)
    # Verificar si la consulta fue exitosa
    if response.status_code == 200:
        prediction = response.json().get("predictions")
    else:
        prediction = "Error"  # Guardar un valor de error en caso de falla

    # Guardar el resultado en el archivo de salida
    with open(output_file, mode='a', newline='') as file:
        writer = csv.writer(file)
        #siniestro_data = pd.DataFrame.from_dict(siniestro_data)
        siniestro = siniestro_data[0]
        writer.writerow([siniestro["claim_id"], prediction])

    print(f"Predicción para claim_id {siniestro['claim_id']}: {prediction}")
