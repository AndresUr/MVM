# Realizamos las importaciones de las librerias a utilziar
import streamlit as st
import requests 
import pandas as pd
import base64

#definimos la url del API a la cual vamos a realizar la peticion
url = "http://localhost:7071/api/Api_MVM"

#definimos una funcion la cual permita descargar los datos enformato csv
def descargar_csv(dataframe):
                csv = dataframe.to_csv(index=False)
                b64 = base64.b64encode(csv.encode()).decode()
                href = f'<a href="data:file/csv;base64,{b64}" download="dataframe.csv">Download CSV</a>'
                return href

def main():
    st.title("Consulta de Vistas MySQL")

    #definimos las siguientes opciones de consulta de los datos, donde cada opcion llama una vista diferente a la base de datos,
    #dependiendo de la seleccion se realiza la peticion al API
    consulta = st.selectbox("Selecciona una opción:",
                            ["Informacion General por Empleado", "Salario por Departamento y Area Trabajo", "Cantidad Empleados por Departamento", "Salario Promedio por Departamento", "Informacion General de los Salarios"])
    params = {'consulta': consulta}

    #en el presente bloque se realiza la peticion al API, la cual es la encargada de validar la consulta seleccionada y se conecta a la BD de mysql para 
    #asi devolver los datos.
    try:
        
        response = requests.post(url, json=params)
        
        if response.status_code == 200:            
            json_data = response.json()  
            dataframe = pd.DataFrame(json_data)

            st.write("Resultados:")

            #Una vez obtenidos los datos se proceden a mostrar en streamlit
            with st.container():                 
                st.dataframe(dataframe,use_container_width=True) 
                st.markdown(descargar_csv(dataframe), unsafe_allow_html=True)
        else:
            st.error("Error al llamar a la función de Azure.")

    except requests.exceptions.RequestException as e:
        st.error(f"Error al llamar a la función de Azure: {e}")
    

if __name__ == "__main__":
    main()
