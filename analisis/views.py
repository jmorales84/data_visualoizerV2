from rest_framework.response import Response
from rest_framework.decorators import api_view
from django.views.decorators.csrf import csrf_exempt
import os
import pandas as pd
import arff
import random

@api_view(['POST'])
@csrf_exempt
def upload_arff(request):
    try:
        if 'file' not in request.FILES:
            return Response({"error": "No se proporcionó ningún archivo"})
        
        file = request.FILES['file']
        if not file.name.endswith('.arff'):
            return Response({"error": "El archivo debe tener extensión .arff"})
        
        # Guardar archivo
        base_path = os.path.join(os.path.dirname(__file__), "..", "..", "datasets", "NSL-KDD")
        os.makedirs(base_path, exist_ok=True)
        
        file_path = os.path.join(base_path, file.name)
        with open(file_path, 'wb+') as destination:
            for chunk in file.chunks():
                destination.write(chunk)
        
        return Response({
            "mensaje": "Archivo subido correctamente", 
            "archivo": file.name,
        })
    
    except Exception as e:
        return Response({"error": str(e)})

@api_view(['GET'])
def visualizar_dataset(request):
    try:
        archivo_seleccionado = request.GET.get('archivo', '')
        porcentaje = request.GET.get('porcentaje', 10)
        filas_muestra = request.GET.get('filas_muestra', 1000)
        
        # Convertir parámetros a enteros con valores por defecto
        try:
            porcentaje = int(porcentaje)
        except (ValueError, TypeError):
            porcentaje = 10
            
        try:
            filas_muestra = int(filas_muestra)
        except (ValueError, TypeError):
            filas_muestra = 1000
        
        # Validar parámetros
        porcentaje = max(1, min(100, porcentaje))
        filas_muestra = max(100, min(10000, filas_muestra))
        
        # Tu función main con los parámetros de muestreo
        data = main(archivo_seleccionado, porcentaje, filas_muestra)
        return Response(data)
    
    except Exception as e:
        return Response({"error": f"Error en visualizar_dataset: {str(e)}"})

# Función para cargar y procesar el dataset
def main(archivo_seleccionado="", porcentaje=10, filas_muestra=1000):
    try:
        # Ruta base del dataset
        base_path = os.path.join(os.path.dirname(__file__), "..", "..", "datasets", "NSL-KDD")
        
        # Verificar que se haya proporcionado un archivo
        if not archivo_seleccionado:
            return {"error": "No se ha proporcionado ningún archivo para visualizar."}
        
        arff_file = os.path.join(base_path, archivo_seleccionado)
        
        # Verificar existencia del archivo .arff
        if not os.path.exists(arff_file):
            return {"error": f"El archivo {archivo_seleccionado} no se encuentra en el servidor."}

        # Leer dataset .arff
        with open(arff_file, 'r', encoding='utf-8') as train_set:
            data = arff.load(train_set)

        # Parsear los atributos y datos
        atributos = [attr[0] for attr in data['attributes']]
        todas_las_filas = data['data']
        
        # Aplicar muestreo según el porcentaje
        if porcentaje < 100:
            num_filas_muestra = max(1, int(len(todas_las_filas) * porcentaje / 100))
            num_filas_muestra = min(num_filas_muestra, filas_muestra)
            # Muestreo aleatorio
            filas_seleccionadas = random.sample(todas_las_filas, num_filas_muestra)
        else:
            filas_seleccionadas = todas_las_filas[:filas_muestra]
        
        df = pd.DataFrame(filas_seleccionadas, columns=atributos)

        # Convertir las primeras filas a formato legible (máximo 50 para la paginación)
        primeras_filas = df.head(50).replace({float('nan'): None}).to_dict(orient="records")

        # Resultado final
        return {
            "mensaje": f"Dataset {archivo_seleccionado} cargado correctamente.",
            "total_filas": len(todas_las_filas),
            "filas_muestra": len(filas_seleccionadas),
            "total_columnas": len(df.columns),
            "columnas": list(df.columns),
            "primeras_filas": primeras_filas,
            "archivo_actual": archivo_seleccionado,
            "porcentaje_cargado": porcentaje
        }

    except Exception as e:
        return {"error": f"Error al cargar dataset: {str(e)}"}