import logging
import functions_framework
import pymysql
import json
import io
import os
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.cloud import storage


# 🔧 Conexión a MySQL
def get_connection():
    conn = pymysql.connect(
        user="zeussafety-2024",
        password="ZeusSafety2025",
        db="Zeus_Safety_Data_Integration",
        unix_socket="/cloudsql/stable-smithy-435414-m6:us-central1:zeussafety-2024",
        cursorclass=pymysql.cursors.DictCursor
    )

    # Para establecer la zona horaria a UTC-5
    with conn.cursor() as cursor:
        cursor.execute("SET time_zone = '-05:00'")
    return conn

# Mapeo de valores del parámetro 'listado' a los nombres de los procedimientos almacenados
LISTADO_PROCEDIMIENTOS = {
    'general': 'sp_listado_solicitudes_respuestas',
    'ventas': 'sp_listado_solicitudes_respuestas_ventas',
    'marketing': 'sp_listado_solicitudes_respuestas_marketing',
    'logistica': 'sp_listado_solicitudes_respuestas_logistica',
    'facturacion': 'sp_listado_solicitudes_respuestas_facturacion',
    'importacion': 'sp_listado_solicitudes_respuestas_importacion',
    'administracion': 'sp_listado_solicitudes_respuestas_administracion',
    'sistemas': 'sp_listado_solicitudes_respuestas_sistemas',
    'gerencia': 'sp_listado_solicitudes_respuestas_gerencia',
    'rrhh': 'sp_listado_solicitudes_respuestas_rrhh',
}

API_TOKEN = "https://api-verificacion-token-2946605267.us-central1.run.app"

# 📦 Función HTTP principal
@functions_framework.http
def registrosolicitudeseincidencias_R(request):
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET,POST,PUT, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type/Authorization',
    }

    try:
        # Obtener el token del header Authorization
        auth_header = request.headers.get("Authorization")
        
        # Log para debugging
        logging.info(f"Authorization header recibido: {auth_header[:50] if auth_header else 'None'}...")
        
        # Validar que el token exista
        if not auth_header:
            return (json.dumps({"error": "Token no proporcionado"}), 401, headers)
        
        # Preparar headers para la verificación del token
        token_headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header
        }
        
        # Log para debugging
        logging.info(f"Verificando token en: {API_TOKEN}")
        logging.info(f"Headers enviados: Authorization={auth_header[:50]}...")
        
        # Verificar el token con la API de autenticación
        try:
            # Enviar POST sin body (solo headers)
            response = requests.post(API_TOKEN, headers=token_headers, timeout=10)
            
            # Log para debugging
            logging.info(f"Respuesta de token API: status={response.status_code}, body={response.text[:200]}")
            
            if response.status_code != 200:
                # transformamos json a diccionarios
                error_response = response.json()
                if "error" in error_response:
                    error_msg = error_response["error"]
                logging.warning(f"Token no autorizado: {error_msg}")
                return (json.dumps({"error": error_msg}), 401, headers)
        except requests.exceptions.RequestException as e:
            # Error de conexión o timeout
            logging.error(f"Error al verificar token: {str(e)}")
            return (json.dumps({"error": f"Error al verificar token: {str(e)}"}), 503, headers)
    except Exception as e:
        return (json.dumps({"error": str(e)}), 500, headers)
    
    if request.method == "OPTIONS":
        return ("", 204, headers)

    try:
        if request.method == "GET":
            # Retorna el listado de solicitudes. La lógica interna (get_solicitudes_incidencias_r)
            # ahora gestiona los diferentes listados vía query parameter 'listado'.
            return get_solicitudes_incidencias_r(request, headers)
            
        elif request.method == 'POST':
            # -------------------------------------------------------------------
            # LÓGICA DE DIFERENCIACIÓN
            # -------------------------------------------------------------------
            
            # Intenta obtener el JSON para la petición de número de solicitud
            try:
                request_json = request.get_json(silent=True)
            except:
                request_json = None
            
            # Caso 1: Petición de Número de Solicitud (Viene como JSON con 'area')
            if request_json and 'area' in request_json:
                area = request_json['area']
                conn = get_connection()
                try:
                    # Usamos el área recibida para generar el siguiente número
                    numero_solicitud = generar_numero_solicitud(area.upper(), conn)
                    if numero_solicitud:
                        return (json.dumps({'numeroSolicitud': numero_solicitud}), 200, headers)
                    else:
                        return (json.dumps({'error': 'Área no válida para generar número.'}), 400, headers)
                finally:
                    conn.close()
            
            # Caso 2: Petición de Registro (Viene como form-data, o JSON para Reprogramación)
            elif request.args.get('accion') == 'reprogramar':
                return registrar_reprogramacion_r(request, headers)
            else:
                # Si no es JSON de número ni Reprogramar, asumimos que es el registro completo
                # NOTA: Debemos asegurar que el frontend envíe 'NUMERO_SOLICITUD' pre-generado
                return insertar_solicitudes_incidencias_r(request, headers)

        elif request.method == 'PUT':
            # Diferencia entre actualizar una SOLICITUD y una REPROGRAMACIÓN
            if request.args.get('accion') == 'reprogramar':
                return actualizar_reprogramacion_r(request, headers)
            if request.args.get('accion') == 'requerimiento':
                return actualizar_requerimiento_solicitudes_r(request, headers)
            else:
                # Si no se especifica 'accion', asume que es una actualización de solicitud
                return actualizar_solicitudes_indicencias_r(request, headers)
            
        else:
            return (json.dumps({'error': 'Método no permitido'}), 405, headers)
    except Exception as e:
        return (json.dumps({'error': str(e)}), 500, headers)


# Manejo de prefijos por AREA 
def generar_numero_solicitud(area, conn):
    prefijos = {
        'MARKETING': 'MK',
        'LOGISTICA': 'LOG',
        'VENTAS': 'VEN',
        'SISTEMAS': 'SIS',
        'ADMINISTRACION': 'ADMIN',
        'FACTURACION': 'FAC',
        'IMPORTACION': 'IMP',
        'GERENCIA': 'GER',
        'RECURSOS HUMANOS': 'RRHH'
    }

    prefijo = prefijos.get(area)
    if not prefijo:
        return None  # Área no válida

    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT MAX(CAST(SUBSTRING_INDEX(NUMERO_SOLICITUD, '_', -1) AS UNSIGNED)) AS ultimo_numero
            FROM solicitudes
            WHERE NUMERO_SOLICITUD LIKE %s
        """, (f"{prefijo}_%",))
        resultado = cursor.fetchone()
        ultimo_numero = resultado['ultimo_numero'] or 0
        nuevo_numero = ultimo_numero + 1
        numero_formateado = str(nuevo_numero).zfill(2)
        return f"{prefijo}_{numero_formateado}"


def get_solicitudes_incidencias_r(request, headers):
    """
    Retorna el listado de solicitudes/incidencias.
    Utiliza el parámetro 'listado' de la query string para determinar 
    el procedimiento almacenado a llamar.
    """
    
    # 1. Obtener el parámetro 'listado' o usar 'general' por defecto
    listado_key = request.args.get('listado', 'general').lower()
    
    # 2. Obtener el nombre del procedimiento almacenado
    nombre_procedimiento = LISTADO_PROCEDIMIENTOS.get(listado_key)
    
    if not nombre_procedimiento:
        error_msg = f"Valor de 'listado' no válido: {listado_key}. Opciones válidas: {', '.join(LISTADO_PROCEDIMIENTOS.keys())}"
        return (json.dumps({'error': error_msg}), 400, headers)

    conn = None
    try:
        # Se asume la función get_connection() devuelve una conexión con un cursor tipo DictCursor o similar.
        # Si se usa un cursor normal, se necesita lógica adicional para mapear a diccionarios.
        conn = get_connection() 
        with conn.cursor() as cursor:
            # Llama al procedimiento almacenado dinámicamente
            cursor.callproc(nombre_procedimiento)
            resultados = cursor.fetchall()
            
            # Lógica para ordenar las claves de cada reprogramación (se mantiene igual)
            for resultado in resultados:
                if 'REPROGRAMACIONES' in resultado and resultado['REPROGRAMACIONES']:
                    try:
                        reprogramaciones_list = json.loads(resultado['REPROGRAMACIONES'])
                        
                        # Definir el orden deseado de las claves
                        orden_deseado = [
                            'ID_REPROGRAMACION',
                            'FECHA_REPROGRAMACION',
                            'RESPUESTA_REPROG',
                            'FH_RESPUESTA',
                            'INFORME_REPROG',
                            'FH_INFORME'
                        ]
                        
                        # Crear una nueva lista de diccionarios ordenados
                        reprogramaciones_ordenadas = []
                        for reprog in reprogramaciones_list:
                            # Usar un diccionario ordenado para forzar el orden
                            ordered_dict = {key: reprog.get(key) for key in orden_deseado}
                            reprogramaciones_ordenadas.append(ordered_dict)
                            
                        resultado['REPROGRAMACIONES'] = reprogramaciones_ordenadas
                        
                    except (json.JSONDecodeError, TypeError) as e:
                        # Mejor registrar el error en un log en lugar de solo imprimir
                        print(f"Error al decodificar o procesar JSON para REPROGRAMACIONES en {nombre_procedimiento}: {e}")
                        resultado['REPROGRAMACIONES'] = []
            
        return (json.dumps(resultados, default=str), 200, headers)
    
    except Exception as e:
        # Registrar error de la base de datos o conexión
        return (json.dumps({'error': f"Error en la base de datos al llamar a {nombre_procedimiento}: {e}"}), 500, headers)
    finally:
        if conn:
            conn.close()



## Función de Subida a Cloud Storage
# Variables globales para el cliente y el bucket de GCS
storage_client = storage.Client()
BUCKET_NAME = "archivos_sistema"
GCS_FOLDER = "incidencias_areas_zeus"

def upload_to_gcs(file):
    """
    Sube un archivo a Google Cloud Storage y devuelve la URL pública.
    Args: file: El objeto de archivo multipart/form-data.
    Returns: La URL del archivo subido o None si hay un error.
    """
    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        # La ruta del archivo en el bucket
        object_name = f"{GCS_FOLDER}/{file.filename}"
        blob = bucket.blob(object_name)

        # Sube el archivo directamente
        blob.upload_from_file(file, content_type=file.content_type)
        
        # Genera la URL pública
        gcs_url = f"https://storage.googleapis.com/{BUCKET_NAME}/{object_name}"
        return gcs_url
    except Exception as e:
        print(f"Error al subir a Cloud Storage: {e}")
        return None


def insertar_solicitudes_incidencias_r(request, headers):
    conn = get_connection()
    informe_link = None 

    # Manejo de la subida de archivos (ahora opcional)
    informe_file = request.files.get('informe')
    if informe_file and informe_file.filename != '':
        # Llamamos a la nueva función para subir a Cloud Storage
        informe_link = upload_to_gcs(informe_file)
        if not informe_link:
            return (json.dumps({'error': 'Error al subir el archivo a Google Cloud Storage.'}), 500, headers)

    # Obtención de los otros datos del formulario
    data = request.form

    try:
        with conn.cursor() as cursor:
            # ✅ QUITAR FECHA_CONSULTA de la lista de columnas y valores
            sql = """
                INSERT INTO solicitudes (
                    REGISTRADO_POR, NUMERO_SOLICITUD, AREA, RES_INCIDENCIA, 
                    REQUERIMIENTOS, INFORME, AREA_RECEPCION, ESTADO
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            valores = (
                data.get('REGISTRADO_POR'),
                data.get('NUMERO_SOLICITUD'),
                data.get('AREA'),
                data.get('RES_INCIDENCIA'),
                data.get('REQUERIMIENTOS'),
                informe_link,  # Usamos el enlace de Cloud Storage
                data.get('AREA_RECEPCION'),
                data.get('ESTADO')
            )
            
            cursor.execute(sql, valores)
            conn.commit()
    
        return (json.dumps({'mensaje': 'Solicitud registrada correctamente', 'link_informe': informe_link}), 200, headers)
    
    except Exception as e:
        conn.rollback()
        return (json.dumps({'error': f"Error en la base de datos: {e}"}), 500, headers)
    finally:
        conn.close()


def actualizar_requerimiento_solicitudes_r(request, headers):
    conn = get_connection()
    data = request.form if request.form else request.get_json(silent=True)

    try:
        id_solicitud = data.get('ID_SOLICITUD')

        # Manejo de archivos para INFORME_2 e INFORME_3
        informe2_file = request.files.get('informe2')
        informe3_file = request.files.get('informe3')

        informe2_link = None
        informe3_link = None

        if informe2_file and informe2_file.filename != '':
            informe2_link = upload_to_gcs(informe2_file)

        if informe3_file and informe3_file.filename != '':
            informe3_link = upload_to_gcs(informe3_file)

        # Si no hay archivo, tomamos el valor enviado en JSON/form
        requerimiento2 = data.get('REQUERIMIENTO_2')
        requerimiento3 = data.get('REQUERIMIENTO_3')
        informe2 = informe2_link if informe2_link else data.get('INFORME_2')
        informe3 = informe3_link if informe3_link else data.get('INFORME_3')

        if not id_solicitud:
            return (json.dumps({'error': 'ID_SOLICITUD es obligatorio'}), 400, headers)

        with conn.cursor() as cursor:
            # Llamamos al procedimiento almacenado
            cursor.callproc(
                'actualizar_requerimientos',
                (id_solicitud, requerimiento2, informe2, requerimiento3, informe3)
            )
            conn.commit()

            # Recuperamos los valores actualizados
            cursor.execute("""
                SELECT 
                    ID_SOLICITUD,
                    REQUERIMIENTOS,
                    REQUERIMIENTO_2,
                    INFORME_2,
                    REQUERIMIENTO_3,
                    INFORME_3
                FROM solicitudes
                WHERE ID_SOLICITUD = %s
            """, (id_solicitud,))
            solicitud = cursor.fetchone()

        return (
            json.dumps({
                'mensaje': 'Solicitud actualizada correctamente',
                'solicitud': solicitud
            }),
            200,
            headers
        )

    except Exception as e:
        conn.rollback()
        return (json.dumps({'error': f"Error en la base de datos: {e}"}), 500, headers)
    finally:
        conn.close()



def actualizar_solicitudes_indicencias_r(request, headers):
    conn = None
    informe_link = None
    
    # Manejo de la subida de archivos (ahora opcional)
    informe_file = request.files.get('informe')
    if informe_file and informe_file.filename != '':
        # Se utiliza la función para subir a Google Cloud Storage
        informe_link = upload_to_gcs(informe_file)
        if not informe_link:
            return (json.dumps({'error': 'Error al subir el archivo a Google Cloud Storage.'}), 500, headers)

    data = request.form
    
    # 🔑 VERIFICAR SI SE DEBE MANTENER EL INFORME EXISTENTE
    mantener_informe = data.get('mantener_informe') == 'true'
    informe_existente = data.get('INFORME_EXISTENTE')
    
    # Si no hay nuevo archivo pero se debe mantener el existente
    if informe_link is None and mantener_informe and informe_existente:
        informe_link = informe_existente
    
    print(">> DEBUG - Actualizar Respuesta:")
    print(f"   Nuevo archivo subido: {informe_link if informe_file else 'No'}")
    print(f"   Mantener informe existente: {mantener_informe}")
    print(f"   Informe existente URL: {informe_existente}")
    print(f"   Informe final a enviar al SP: {informe_link}")
    
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            id_solicitud = data.get('ID_SOLICITUD')
            if not id_solicitud:
                return (json.dumps({'error': 'El ID_SOLICITUD es obligatorio para la actualización.'}), 400, headers)

            respondido_por = data.get('RESPONDIDO_POR')
            respuesta = data.get('RESPUESTA')
            estado = data.get('ESTADO')

            # Llama al procedimiento "upsert" con el informe preservado si corresponde
            cursor.callproc('sp_upsert_respuesta', (id_solicitud, respondido_por, respuesta, informe_link, estado))
            conn.commit()
            
        return (
            json.dumps({
                'mensaje': 'Respuesta actualizada o registrada correctamente.', 
                'link_informe': informe_link or 'Sin cambios'
            }), 
            200, 
            headers
        )

    except Exception as e:
        if conn:
            conn.rollback()
        print(f">> ERROR en actualización: {str(e)}")
        return (json.dumps({'error': f"Error en la base de datos: {e}"}), 500, headers)
    finally:
        if conn:
            conn.close()


def registrar_reprogramacion_r(request, headers):
    conn = None
    informe_link = None
    informe_file = request.files.get('informe')

    if informe_file and informe_file.filename != '':
        # Ahora se usa la función para subir a Google Cloud Storage
        informe_link = upload_to_gcs(informe_file)
        if not informe_link:
            return (json.dumps({'error': 'Error al subir el archivo a Google Cloud Storage.'}), 500, headers)

    data = request.form
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            id_respuesta = data.get('ID_RESPUESTA')
            fecha_reprogramacion = data.get('FECHA_REPROGRAMACION')
            respuesta = data.get('RESPUESTA')
            
            # Llama al procedimiento de inserción
            cursor.callproc('sp_insert_reprogramacion', (id_respuesta, fecha_reprogramacion, respuesta, informe_link))
            conn.commit()
            
        return (json.dumps({'mensaje': 'Nueva reprogramación registrada correctamente.', 'link_informe': informe_link}), 200, headers)
    
    except Exception as e:
        if conn: conn.rollback()
        return (json.dumps({'error': f"Error en la base de datos: {e}"}), 500, headers)
    finally:
        if conn: conn.close()


def actualizar_reprogramacion_r(request, headers):
    conn = None
    informe_link = None

    # Manejo del archivo (opcional)
    informe_file = request.files.get('informe')
    if informe_file and informe_file.filename != '':
        # Lógica de subida a Cloud Storage
        informe_link = upload_to_gcs(informe_file)
        if not informe_link:
            return (json.dumps({'error': 'Error al subir el archivo a Google Cloud Storage.'}), 500, headers)

    data = request.form

    # 🔎 Debug: ver todas las claves que llegaron
    print(">> FORM DATA KEYS:")
    for k in data.keys():
        print(f"   {k} = {data[k]}")

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            id_reprogramacion = data.get('ID_REPROGRAMACION')
            if not id_reprogramacion:
                return (json.dumps({'error': 'El ID_REPROGRAMACION es obligatorio para la actualización.'}), 400, headers)

            # Capturar respuesta
            respuesta = data.get('RESPUESTA')
            
            # 🔑 VERIFICAR SI SE DEBE MANTENER EL INFORME EXISTENTE
            mantener_informe = data.get('mantener_informe') == 'true'
            informe_existente = data.get('INFORME_EXISTENTE')
            
            print(">> RESPUESTA RECIBIDA:", respuesta)
            print(">> MANTENER INFORME:", mantener_informe)
            print(">> INFORME EXISTENTE:", informe_existente)
            print(">> NUEVO INFORME_LINK:", informe_link)

            # 🔧 CONSTRUIR SQL DINÁMICAMENTE
            # Solo actualizar los campos que realmente necesitan cambiar
            campos_actualizar = []
            valores = []

            # Siempre actualizar RESPUESTA si viene
            if respuesta is not None:
                campos_actualizar.append("RESPUESTA = %s")
                valores.append(respuesta)

            # Solo actualizar INFORME si:
            # 1. Hay un nuevo archivo (informe_link no es None), O
            # 2. NO se debe mantener el existente (sobrescribir con NULL)
            if informe_link is not None:
                # Hay nuevo archivo
                campos_actualizar.append("INFORME = %s")
                valores.append(informe_link)
            elif mantener_informe and informe_existente:
                # Mantener el existente explícitamente
                campos_actualizar.append("INFORME = %s")
                valores.append(informe_existente)
            # Si no hay nuevo archivo Y no se indica mantener, NO se actualiza el campo INFORME
            # (se preserva el valor actual en la BD)

            if not campos_actualizar:
                return (json.dumps({'error': 'No hay campos para actualizar.'}), 400, headers)

            # Agregar ID al final
            valores.append(id_reprogramacion)

            sql = f"UPDATE reprogramaciones SET {', '.join(campos_actualizar)} WHERE ID_REPROGRAMACION = %s"
            
            print(">> SQL GENERADO:", sql)
            print(">> VALORES:", valores)

            cursor.execute(sql, valores)
            conn.commit()

        return (
            json.dumps({
                'mensaje': 'Reprogramación actualizada correctamente.',
                'link_informe': informe_link or informe_existente or 'Sin cambios'
            }),
            200,
            headers
        )
    except Exception as e:
        if conn:
            conn.rollback()
        print(">> ERROR:", str(e))
        return (json.dumps({'error': f"Error en la base de datos: {e}"}), 500, headers)
    finally:
        if conn:
            conn.close()




