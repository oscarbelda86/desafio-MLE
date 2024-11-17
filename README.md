# desafio-MLE

Proyecto de FastAPI para inferencia de modelo de machine learning y procesamiento de pipelines.

## Instalación
1. Clonar repositorio:
```bash
git clone https://github.com/oscarbelda86/desafio-MLE.git
```
3. Instalar dependencias:
```bash
pip install -r requirements.txt
```
5. Correr el servidor FastAPI:
```bash
uvicorn app.main:app
```
## Importante: agregar archivos .pkl (pipelines y modelo) a carpeta app/models

## Correr test_api
1. Tener claims_dataset.csv en directorio principal
2. Tener servidor FastAPI corriendo
3. ```python
   python test_api.py
   ```


