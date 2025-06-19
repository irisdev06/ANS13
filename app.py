import streamlit as st
from views.courier import (
    cargar_datos,
    rellenar_fecha_recibido,
    calcular_indicador,
    agregar_termino,
    generarcol_proveedor,
    generar_excel,
    agregar_columnas_vacias,
    obtener_dfs_filtrados,
    obtener_dfs_por_proveedor, 
    generar_tabla_resumen,
    generar_excel
)


st.title("📊 ANS13")
archivo = st.file_uploader("Sube el archivo CSV con las fechas", type=["csv"])
if archivo is not None:
    datos = cargar_datos(archivo)
    datos = rellenar_fecha_recibido(datos)
    datos = calcular_indicador(datos)
    datos = agregar_termino(datos)
    datos = generarcol_proveedor(datos)
    datos = agregar_columnas_vacias(datos)
    df_consolidado, df_courier = obtener_dfs_filtrados(datos)
    dfs_proveedores = obtener_dfs_por_proveedor(df_courier)
    resume = generar_tabla_resumen(datos)
    
    excel_bytes = generar_excel(datos)
    
    # Botón para descargar el archivo
    st.download_button(
        label="⬇️ Descargar Excel por Medio de Envío",
        data=excel_bytes,
        file_name="CORRESPONDENCIA.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
