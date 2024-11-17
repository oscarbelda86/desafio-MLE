from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import pandas as pd
from app.pipeline import pipeline_manager
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import csv
from datetime import datetime


LOG_FILE = "api_log.csv"

# Función para escribir en el log
def log_prediction(data, prediction):
    with open(LOG_FILE, mode='a', newline='') as file:
        writer = csv.writer(file)
        row = [datetime.now(), data, prediction]
        writer.writerow(row)


# Initialize router
router = APIRouter()

# Modelo de datos de entrada
class ClaimData(BaseModel):
    claim_id: int
    marca_vehiculo: str
    antiguedad_vehiculo: int
    tipo_poliza: int
    taller: int
    partes_a_reparar: int
    partes_a_reemplazar: int

@router.post("/")
async def predict(claim: ClaimData):
    try:
        claim = claim.__dict__
        # Check if 'tipo_poliza' is 4
        if claim.get("tipo_poliza") == 4:
            return {"predictions": [-1]}
        
        # Preprocess and predict
        input_df = pd.DataFrame([claim])
        preprocessed_data = pipeline_manager.preprocess_data(input_df)
        pipeline_manager._load_model()
        predictions = pipeline_manager.model.predict(preprocessed_data.drop(['claim_id'],axis=1))

        log_prediction(claim, predictions)

        return {"predictions": predictions.tolist()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
