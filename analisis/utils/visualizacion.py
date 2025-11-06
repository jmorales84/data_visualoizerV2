import pandas as pd
import arff
import os

def main():
    try:
        # Ruta base del dataset (ajusta si tu carpeta cambia)
        base_path = os.path.join(os.path.dirname(__file__), "../../..", "datasets", "NSL-KDD")

        # Verificar existencia del archivo .arff
        arff_file = os.path.join(base_path, "KDDTrain+.arff")
        if not os.path.exists(arff_file):
            return {"error": "El archivo KDDTrain+.arff no se encuentra en datasets/NSL-KDD/"}

        # Leer dataset .arff
        with open(arff_file, 'r') as train_set:
            data = arff.load(train_set)

        # Parsear los atributos y datos
        atributos = [attr[0] for attr in data['attributes']]
        df = pd.DataFrame(data['data'], columns=atributos)

        # Convertir las primeras filas a formato legible
        primeras_filas = df.head(5).to_dict(orient="records")

        # Resultado final
        return {
            "mensaje": "Dataset cargado correctamente.",
            "total_filas": len(df),
            "total_columnas": len(df.columns),
            "columnas": list(df.columns),
            "primeras_filas": primeras_filas
        }

    except Exception as e:
        return {"error": str(e)}
