from fastapi import FastAPI
from app.routes import predict,health
from app.pipeline import pipeline_manager

app = FastAPI(title="API Inferencia seguros autos")

# Include routes
app.include_router(predict.router, prefix="/predict", tags=["Prediction"])
app.include_router(health.router, prefix="/health", tags=["Health"])

@app.on_event("startup")
def load_pipelines():
    pipeline_files = [f'app/models/pipeline_{i}.pkl' for i in range(1, 5)] # cambiar dependiendo de usar o no pipeline 5
    pipeline_manager.load_all_pipelines(pipeline_files=pipeline_files)
    pipeline_manager._load_model()
    print("Pipelines y modelo cargados exitosamente!")