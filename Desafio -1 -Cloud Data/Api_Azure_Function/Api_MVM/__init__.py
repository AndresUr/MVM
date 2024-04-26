#realizamos la importacion de las librerias que van a ser usadas en la construccion del api
import logging
import pymysql
import azure.functions as func
import pandas as pd


#la funcion connect_to_database() establece una conexión a la base de datos MySQL utilizando las credenciales proporcionadas.
#esto mediante conector pymysql
def connect_to_database():
    connection = pymysql.connect(
        host='test-facapi-da12sol. 2A122*w3e__wbr2*y.mx-eastus-1.mysql.amazonaws.com',
        user='admin',
        password='d/¿12la*Qc1tr__.K',
        database='dwh_tp'
    )
    return connection

#funcion load_data_bd ejecuta una consulta SQL hacia la base de datos,para la consulta de las vistas almacenadas en la base de datos de mysql, al igual 
#esta funcion maneja la apertura y cierre de conexion a la vez que maneja las excepciones que lleguen a generarse. al igual retorna los resultados en formato json
def load_data_bd(query):    
    try:            
        connection = connect_to_database()              
        
        df = pd.read_sql(query, con=connection)        

        return df.to_json(orient='records')                
        #return df
    except Exception as e:
        print(f"Error en {e}")
    finally:
        
        connection.close()

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    #Recibimos el parametro consulta enviado desde el front en streamlit,
    req_body = req.get_json()
    consulta = req_body.get('consulta', None)
    
    #validamos si no existe parametro consulta en la peticion realizada al API
    if not consulta:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            consulta = req_body.get('consulta')

    # se valida el tipo de consulta a realizar, dependiendo de cada opcion se establece un query para la consulta de la vista correspondiente en MYSQL
    if consulta == "Informacion General por Empleado":
        query = "SELECT * FROM EmployeesDepartmentJob;"      
    elif consulta == "Salario por Departamento y Area Trabajo":        
        query = "SELECT * FROM SalaryMinMaxDepartmentAndJob;"
    elif consulta == "Cantidad Empleados por Departamento":
        query = "SELECT * FROM DepartmentsMostEmp;"
    elif consulta == "Salario Promedio por Departamento":        
        query = "SELECT * FROM SalaryAVGDepartment;"
    elif consulta == "Informacion General de los Salarios":
        query = "SELECT * FROM SalaryDepJob;"
    
    #realizamos la obtencion de los datos con el query seleccionado, y una vez obtenido los resultados se devuelven al front que consulta el api
    # se realiza una captura de excepciones para evitar algun fallo en el proceso de consulta del API
    try:
        df = load_data_bd(query)
        return func.HttpResponse(df, mimetype="application/json")
    except Exception as e:
        logging.error(f"Error inesperado: {e}")
        return func.HttpResponse("Error interno del servidor", status_code=500)
    