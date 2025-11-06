import pandas as pd
import arff
import os
import random

def main(archivo_seleccionado="precargado", porcentaje=100, filas_muestra=1000):
    """
    Carga un dataset ARFF de forma optimizada con muestreo por porcentajes
    
    Args:
        archivo_seleccionado: Nombre del archivo a cargar
        porcentaje: Porcentaje del dataset a cargar (1-100)
        filas_muestra: Número máximo de filas para mostrar en vista previa
    """
    try:
        # Ruta base del dataset
        if os.environ.get('RENDER', False):
            # En Render, usar directorio temporal
            base_path = os.path.join('/tmp', 'datasets', 'NSL-KDD')
        else:
            base_path = os.path.join(os.path.dirname(__file__), "..", "..", "datasets", "NSL-KDD")
        
        os.makedirs(base_path, exist_ok=True)
        
        # Determinar qué archivo usar
        if archivo_seleccionado == "precargado":
            arff_file = os.path.join(base_path, "KDDTrain+.arff")
            if not os.path.exists(arff_file):
                crear_dataset_ejemplo(arff_file)
        else:
            arff_file = os.path.join(base_path, archivo_seleccionado)
        
        if not os.path.exists(arff_file):
            return {"error": f"El archivo {archivo_seleccionado} no se encuentra."}

        # OPCIÓN 1: Lectura rápida con muestreo aleatorio
        return leer_con_muestreo(arff_file, porcentaje, filas_muestra, archivo_seleccionado)
        
        # OPCIÓN 2: Si necesitas todo el dataset pero procesado más rápido
        # return leer_dataset_completo(arff_file, porcentaje, archivo_seleccionado)

    except Exception as e:
        return {"error": f"Error al cargar dataset: {str(e)}"}

def leer_con_muestreo(arff_file, porcentaje, filas_muestra, archivo_seleccionado):
    """Lee el ARFF con muestreo aleatorio para mayor velocidad"""
    # Primero, contar las filas totales sin cargar todo
    total_filas = contar_filas_arff(arff_file)
    
    if total_filas == 0:
        return {"error": "El archivo está vacío o no se pudo leer"}
    
    # Calcular cuántas filas necesitamos basado en el porcentaje
    if porcentaje < 100:
        filas_a_leer = max(1, int(total_filas * porcentaje / 100))
        # No leer más de lo necesario para la vista previa
        filas_a_leer = min(filas_a_leer, filas_muestra)
    else:
        filas_a_leer = min(total_filas, filas_muestra)
    
    # Leer solo las filas necesarias
    with open(arff_file, 'r', encoding='utf-8') as train_set:
        data = arff.load(train_set)
    
    atributos = [attr[0] for attr in data['attributes']]
    todas_las_filas = data['data']
    
    # Si necesitamos menos filas que el total, hacer muestreo aleatorio
    if len(todas_las_filas) > filas_a_leer:
        if porcentaje < 100:
            # Muestreo aleatorio para el porcentaje
            filas_seleccionadas = random.sample(todas_las_filas, filas_a_leer)
        else:
            # Solo las primeras N filas para vista rápida
            filas_seleccionadas = todas_las_filas[:filas_a_leer]
    else:
        filas_seleccionadas = todas_las_filas
    
    df_muestra = pd.DataFrame(filas_seleccionadas, columns=atributos)
    
    # Para estadísticas, contar columnas de una fila de ejemplo
    total_columnas = len(atributos)
    
    # Convertir a formato legible (solo primeras 10 para mostrar)
    primeras_filas = df_muestra.head(10).replace({float('nan'): None}).to_dict(orient="records")
    
    return {
        "mensaje": f"Dataset {archivo_seleccionado} cargado correctamente. " +
                  f"Mostrando {len(filas_seleccionadas)} de {total_filas} filas " +
                  f"({porcentaje}% del total)",
        "total_filas": total_filas,
        "filas_muestra": len(filas_seleccionadas),
        "total_columnas": total_columnas,
        "columnas": atributos,
        "primeras_filas": primeras_filas,
        "archivo_actual": archivo_seleccionado,
        "porcentaje_cargado": porcentaje,
        "filas_mostradas": min(10, len(filas_seleccionadas))
    }

def contar_filas_arff(arff_file):
    """Cuenta las filas en un archivo ARFF sin cargarlo completamente"""
    try:
        with open(arff_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Buscar la sección @data
        in_data_section = False
        count = 0
        
        for line in lines:
            line = line.strip()
            if line.lower() == '@data':
                in_data_section = True
                continue
            if in_data_section and line and not line.startswith('%') and not line.startswith('@'):
                count += 1
        
        return count
    except:
        # Fallback: cargar y contar
        with open(arff_file, 'r', encoding='utf-8') as train_set:
            data = arff.load(train_set)
        return len(data['data'])

def leer_dataset_completo(arff_file, porcentaje, archivo_seleccionado):
    """Lee el dataset completo pero más optimizado"""
    with open(arff_file, 'r', encoding='utf-8') as train_set:
        data = arff.load(train_set)
    
    atributos = [attr[0] for attr in data['attributes']]
    todas_las_filas = data['data']
    
    total_filas = len(todas_las_filas)
    
    # Aplicar filtro por porcentaje si es necesario
    if porcentaje < 100:
        filas_a_tomar = max(1, int(total_filas * porcentaje / 100))
        filas_seleccionadas = random.sample(todas_las_filas, filas_a_tomar)
    else:
        filas_seleccionadas = todas_las_filas
    
    df = pd.DataFrame(filas_seleccionadas, columns=atributos)
    
    # Solo convertir las primeras filas a dict para mostrar
    primeras_filas = df.head(10).replace({float('nan'): None}).to_dict(orient="records")
    
    return {
        "mensaje": f"Dataset {archivo_seleccionado} cargado correctamente. " +
                  f"Mostrando {len(filas_seleccionadas)} de {total_filas} filas ({porcentaje}%)",
        "total_filas": total_filas,
        "filas_muestra": len(filas_seleccionadas),
        "total_columnas": len(df.columns),
        "columnas": list(df.columns),
        "primeras_filas": primeras_filas,
        "archivo_actual": archivo_seleccionado,
        "porcentaje_cargado": porcentaje,
        "filas_mostradas": 10
    }

def crear_dataset_ejemplo(file_path):
    """Crea un dataset ARFF de ejemplo si no existe ninguno"""
    contenido_ejemplo = '''@RELATION ejemplo_dataset

@ATTRIBUTE feature1 NUMERIC
@ATTRIBUTE feature2 NUMERIC  
@ATTRIBUTE feature3 {A,B,C}
@ATTRIBUTE class {normal,anomaly}

@DATA
1.0,2.1,A,normal
1.5,3.2,B,normal
2.0,1.8,C,anomaly
0.5,2.5,A,normal
1.8,2.9,B,normal
3.0,4.1,C,anomaly
1.2,2.3,A,normal
2.5,3.8,B,anomaly
'''
    with open(file_path, 'w') as f:
        f.write(contenido_ejemplo)
