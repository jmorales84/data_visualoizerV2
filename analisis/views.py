from rest_framework.response import Response
from rest_framework.decorators import api_view
from django.views.decorators.csrf import csrf_exempt
import os
import pandas as pd
import arff

# Ruta para visualizar el dataset
@api_view(['GET'])
def visualizar_dataset(request):
    data = main()
    return Response(data)

# Ruta para subir archivos ARFF
@api_view(['POST'])
@csrf_exempt
def upload_arff(request):
    try:
        if 'file' not in request.FILES:
            return Response({"error": "No se proporcionó ningún archivo"})
        
        file = request.FILES['file']
        if not file.name.endswith('.arff'):
            return Response({"error": "El archivo debe tener extensión .arff"})
        
        # Ruta base del dataset - CORREGIDA para tu estructura
        base_path = os.path.join(os.path.dirname(__file__), "..", "..", "datasets", "NSL-KDD")
        os.makedirs(base_path, exist_ok=True)
        
        # Guardar el archivo
        file_path = os.path.join(base_path, "KDDTrain+.arff")
        with open(file_path, 'wb+') as destination:
            for chunk in file.chunks():
                destination.write(chunk)
        
        return Response({
            "mensaje": "Archivo subido correctamente", 
            "archivo": file.name,
        })
    
    except Exception as e:
        return Response({"error": str(e)})

# Función para cargar y procesar el dataset
def main():
    try:
        # Ruta base del dataset - CORREGIDA para tu estructura
        base_path = os.path.join(os.path.dirname(__file__), "..", "..", "datasets", "NSL-KDD")
        
        # Verificar existencia del archivo .arff
        arff_file = os.path.join(base_path, "KDDTrain+.arff")
        if not os.path.exists(arff_file):
            return {"error": f"El archivo KDDTrain+.arff no se encuentra. Por favor, sube un archivo ARFF."}

        # Leer dataset .arff
        with open(arff_file, 'r', encoding='utf-8') as train_set:
            data = arff.load(train_set)

        # Parsear los atributos y datos
        atributos = [attr[0] for attr in data['attributes']]
        df = pd.DataFrame(data['data'], columns=atributos)

        # Convertir las primeras filas a formato legible
        primeras_filas = df.head(150).replace({float('nan'): None}).to_dict(orient="records")

        # Resultado final
        return {
            "mensaje": "Dataset cargado correctamente.",
            "total_filas": len(df),
            "total_columnas": len(df.columns),
            "columnas": list(df.columns),
            "primeras_filas": primeras_filas
        }

    except Exception as e:
        return {"error": f"Error al cargar dataset: {str(e)}"}