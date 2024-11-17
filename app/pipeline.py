import numpy as np
import dill
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from typing import Dict, Any
import logging
import traceback

diccionario_imputacion = {
    'log_total_piezas': 1.4545,
    'marca_vehiculo_encoded': 0,
    'valor_vehiculo': 3560,
    'valor_por_pieza': 150,
    'antiguedad_vehiculo': 1,
    'tipo_poliza': 1,
    'taller': 1,
    'partes_a_reparar': 3,
    'partes_a_reemplazar': 1
}

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PipelineManager:
    def __init__(self):
        self.pipelines: Dict[int, Any] = {}
        self.lock = threading.Lock()
        self._load_model()
        
    def _load_model(self):
        """Load the prediction model once during initialization."""
        try:
            with open('app/models/linnear_regression.pkl', 'rb') as f:
                self.model = dill.load(f)
            logger.info("Model loaded successfully")
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            raise

    def load_pipeline(self, filename: str) -> tuple:
        """Load a single pipeline with error handling."""
        try:
            with open(filename, 'rb') as f:
                pipeline = dill.load(f)
            pipeline_num = int(filename.split('_')[1].split('.')[0])
            return pipeline_num, pipeline
        
        except Exception as e:
            logger.error(f"Error loading pipeline {filename}: {e}")
            raise
    
    def load_all_pipelines(self, pipeline_files: list) -> None:
        """Load all pipelines using ThreadPoolExecutor with proper error handling."""
        # Using ThreadPoolExecutor instead of ProcessPoolExecutor for dill compatibility
        with ThreadPoolExecutor(max_workers=len(pipeline_files)) as executor:
            future_to_file = {
                executor.submit(self.load_pipeline, fname): fname 
                for fname in pipeline_files
            }
            
            for future in as_completed(future_to_file):
                try:
                    pipeline_num, pipeline = future.result()
                    with self.lock:
                        self.pipelines[pipeline_num] = pipeline
                    logger.info(f"Pipeline {pipeline_num} loaded successfully")
                
                except Exception as e:
                    filename = future_to_file[future]
                    logger.error(f"Failed to load pipeline {filename}: {e}")
                    raise

    def apply_pipeline(self, pipeline_num: int, data):
        """Apply a single pipeline with error handling. It also has data imputation"""
        try:
            with self.lock:
                pipeline = self.pipelines[pipeline_num]
                #Conditionals for missing data handling
                if pipeline_num == 1:
                    for column, default_value in diccionario_imputacion.items():
                        if column in data.columns:
                            data[column] = data[column].fillna(default_value)
                    df = pipeline(data)
                
                else:
                    if pipeline_num==2:
                        data['log_total_piezas'] = np.log(
                        data['partes_a_reparar'] + data['partes_a_reemplazar']
                        )
                        df = data
                    else:
                        df = pipeline(data)
                        for column, default_value in diccionario_imputacion.items():
                            if column in df.columns:
                                df[column] = df[column].fillna(default_value)
                    return df
            return pipeline(data)
        except Exception as e:
            logger.error(f"Error applying pipeline {pipeline_num}: {e}")
            raise

    def preprocess_data(self, data):
        """Preprocess data with optimized parallel execution and error handling."""
        try:
            df = data.copy()
            
            # Define pipeline execution groups based on dependencies
            independent_pipelines = [1, 3]  # Can run in parallel
            stage2_pipelines = [2]          # Depends on pipeline 1
            stage3_pipelines = [4]       # Depend on pipelines 2 and 3. Add 5 to list if using pipeline 5
            

            # Stage 1: Execute independent pipelines in parallel
            
            with ThreadPoolExecutor() as executor:
                futures = {
                    executor.submit(self.apply_pipeline, pipeline_num, df): pipeline_num 
                    for pipeline_num in independent_pipelines
                }
                
                results = {}
                for future in as_completed(futures):
                    pipeline_num = futures[future]
                    results[pipeline_num] = future.result()
            
            # Merge results from independent pipelines
            for pipeline_num in independent_pipelines:
                df.update(results[pipeline_num])
            
            # Stage 2: Execute pipeline 2 which depends on pipeline 1
            for pipeline_num in stage2_pipelines:
                df = self.apply_pipeline(pipeline_num, df)
            
            # Stage 3: Execute pipelines 4 and 5 in parallel
            with ThreadPoolExecutor() as executor:
                futures = {
                    executor.submit(self.apply_pipeline, pipeline_num, df): pipeline_num 
                    for pipeline_num in stage3_pipelines
                }
                
                for future in as_completed(futures):
                    df = future.result()
            
            required_columns = [
                'claim_id', 'log_total_piezas', 'marca_vehiculo_encoded',
                'valor_vehiculo', 'valor_por_pieza', 'antiguedad_vehiculo'
            ]
            
            return df[required_columns]
            
        except Exception as e:
            logger.error(f"Error in preprocessing data: {e}")
            error_details = traceback.format_exc()  # Obtener la traza completa del error
            print("Error en preprocess:", error_details)
            raise


pipeline_manager = PipelineManager()