import pandas as pd
import numpy as np
import xlsxwriter
import matplotlib.pyplot as plt
from itertools import cycle, islice
import tempfile
import holidays
from pandas.tseries.offsets import CustomBusinessDay
from datetime import date
from io import BytesIO
import os
from datetime import datetime, timedelta


# Cargar datos desde un archivo CSV o Excel
def cargar_datos(archivo) -> pd.DataFrame:
    # Detectar el tipo de archivo por su extensión
    if archivo.endswith('.csv'):
        # Leer el archivo CSV
        datos = pd.read_csv(archivo, sep=";", encoding="utf-8", on_bad_lines="warn", engine="python")
    elif archivo.endswith('.xlsx'):
        # Leer el archivo Excel
        datos = pd.read_excel(archivo, engine="openpyxl")
    else:
        raise ValueError("El archivo debe ser de tipo .csv o .xlsx")

    # Convertir las columnas de fecha
    datos['FECHA RADICACION'] = pd.to_datetime(datos['FECHA RADICACION'], errors='coerce', dayfirst=False)
    datos['FECHA RECIBIDO CORRESPONDENCIA'] = pd.to_datetime(datos['FECHA RECIBIDO CORRESPONDENCIA'], errors='coerce', dayfirst=False)
    
    return datos


# Rellenar fecha de recibido con fecha actual
def rellenar_fecha_recibido(datos: pd.DataFrame) -> pd.DataFrame:
    fecha_actual = pd.Timestamp(date.today())
    datos['FECHA RECIBIDO CORRESPONDENCIA'] = datos['FECHA RECIBIDO CORRESPONDENCIA'].fillna(fecha_actual)
    return datos

# Diferencia de días
def calcular_indicador(datos: pd.DataFrame) -> pd.DataFrame:
    years = list(set(datos['FECHA RADICACION'].dt.year.dropna().astype(int).tolist() +
                     datos['FECHA RECIBIDO CORRESPONDENCIA'].dt.year.dropna().astype(int).tolist()))
    col_holidays = holidays.Colombia(years=years)

    def dias_habiles(row):
        start = row['FECHA RADICACION'].normalize()
        end = row['FECHA RECIBIDO CORRESPONDENCIA'].normalize()
        if pd.isna(start) or pd.isna(end) or start > end:
            return None  
        bdays = pd.bdate_range(start, end).difference(pd.to_datetime(list(col_holidays.keys())))
        dias = len(bdays) - 1
        return dias if dias >= 0 else 0

    datos["INDICADOR"] = datos.apply(dias_habiles, axis=1)
    return datos

# Evaluar Término
def evaluar_termino(dias: int) -> str:
    return "EN TERMINO" if 0 <= dias < 2 else "FUERA DE TERMINO"

# Crear columna indicador
def agregar_termino(datos: pd.DataFrame) -> pd.DataFrame:
    datos['TERMINO'] = datos['INDICADOR'].apply(evaluar_termino)
    return datos

# Clasificación por Proveedor
def generarcol_proveedor(datos: pd.DataFrame) -> pd.DataFrame:
    proveedor_map = {
        '3 GRUPO JUNTAS DE CALIFICACIÓN': 'BELISARIO',
        '3 GRUPO CENTRO DE EXCELENCIA': 'BELISARIO',
        '4 GRUPO JUNTAS DE CALIFICACIÓN': 'UTMDL',
        '4 GRUPO CENTRO DE EXCELENCIA': 'UTMDL',
        '5 GRUPO CENTRO DE EXCELENCIA': 'BELISARIO397',
        '5 GRUPO JUNTAS DE CALIFICACIÓN': 'BELISARIO397',
        '6 GRUPO CENTRO DE EXCELENCIA': 'GESTAR INNOVACION',
        '6 GRUPO JUNTAS DE CALIFICACIÓN': 'GESTAR INNOVACION',
        'GERENCIA MEDICA EXCELENCIA': 'GER.MED.EXCELENCIA',
        'GERENCIA MEDICA JUNTAS': 'GER.MED.JUNTAS'
    }

    datos['Proveedor'] = datos["DEPENDENCIA QUE ENVIA"].map(proveedor_map).fillna("DESCONOCIDO")
    return datos

COLUMNAS_EXTRA = ['OPORTUNIDAD FINAL', 'OBSERVACIÓN', 'DEFINICION']

def agregar_columnas_vacias(df: pd.DataFrame) -> pd.DataFrame:
    for col in COLUMNAS_EXTRA:
        df[col] = ''
    return df

def obtener_dfs_filtrados(datos: pd.DataFrame):
    df_consolidado = datos[datos['MEDIO DE ENVIO'] != 'Courier'].copy()
    df_courier = datos[datos['MEDIO DE ENVIO'] == 'Courier'].copy()
    return df_consolidado, df_courier

def obtener_dfs_por_proveedor(df_courier: pd.DataFrame):
    if 'Proveedor' not in df_courier.columns:
        return []
    proveedores = df_courier['Proveedor'].dropna().unique()
    return [
        (str(proveedor)[:31], df_courier[df_courier['Proveedor'] == proveedor].copy())
        for proveedor in proveedores
    ]
# --- Funciones para indicadores ---
# Tablas
def generar_tabla_resumen(datos: pd.DataFrame) -> dict:
    proveedores_objetivo = ['UTMDL', 'GESTAR INNOVACION', 'BELISARIO397', 'BELISARIO']
    datos = datos[datos['Proveedor'].isin(proveedores_objetivo)].copy()

    tablas_por_proveedor = {}

    for proveedor in proveedores_objetivo:
        df_prov = datos[datos['Proveedor'] == proveedor].copy()

        resumen = df_prov.groupby(['MES']).agg(
            UNIVERSO=('TERMINO', 'count'),
            FUERA_DE_TERMINO=('TERMINO', lambda x: (x == 'FUERA DE TERMINO').sum()),
            EXCLUSIONES=('OBSERVACIÓN', lambda x: (x.str.contains('EXCLUIR', na=False)).sum() if 'OBSERVACIÓN' in x else 0),
            TERMINOS=('TERMINO', lambda x: (x == 'EN TERMINO').sum())
        ).reset_index()

        # Convertir 'TERMINOS' a numérico (si es necesario)
        resumen['TERMINOS'] = pd.to_numeric(resumen['TERMINOS'], errors='coerce').fillna(0)

        # Calcular porcentaje indicado, redondeando los valores numéricos
        resumen['PORCENTAJE INDICADO'] = (
            (resumen['TERMINOS'] / resumen['UNIVERSO']) * 100
        ).round(2).astype(str) + '%'

        tablas_por_proveedor[proveedor] = resumen

    return tablas_por_proveedor



# Destinatario
def transformar_destinatarios(df_base):
    # Diccionario con las palabras clave y su valor estandarizado
    palabras_clave = {
        'compensar': 'COMPENSAR',
        'colfondos': 'COLFONDOS',
        'seguros bolivar': 'SEGUROS BOLIVAR',
        'coomeva liquidada': 'COOMEVA LIQUIDADA',
        'junta regional': 'JUNTA REGIONAL',
        'unta regional': 'JUNTA REGIONAL',
        'junta nacional': 'JUNTA NACIONAL'
    }
    
    # Función para transformar los destinatarios
    def transformar(x):
        # Convertir el texto a minúsculas para hacer la búsqueda insensible al caso
        x_lower = x.lower()
        
        # Buscar la palabra clave en el texto y devolver el valor estandarizado
        for clave, valor in palabras_clave.items():
            if clave in x_lower:
                return valor
        
        # Si no se encuentra ninguna coincidencia, devolver el valor original
        return x
    
    # Crear la nueva columna transformada en df_base
    df_base['DESTINATARIO_TRANSFORMADO'] = df_base['DESTINATARIO'].apply(transformar)
    
    return df_base

#  Hoja BASE
def generar_hoja_base(datos: pd.DataFrame, writer) -> pd.DataFrame:
    # Filtrar los datos en "Consolidado" y "Courier"
    df_consolidado, df_courier = obtener_dfs_filtrados(datos)

    # Unir los datos de Consolidado y Courier
    df_base = pd.concat([df_consolidado, df_courier], ignore_index=True)
    
    # Aplicar las funciones de procesamiento de datos a df_base
    df_base = rellenar_fecha_recibido(df_base)   # Rellenar fecha de recibido con fecha actual
    df_base = calcular_indicador(df_base)        # Calcular la diferencia de días (INDICADOR)
    df_base = agregar_termino(df_base)           # Crear la columna TERMINO
    df_base = generarcol_proveedor(df_base)      # Clasificación por proveedor
    df_base = transformar_destinatarios(df_base)

    # Si df_base no está vacío, crear la hoja BASE en el archivo Excel
    if not df_base.empty:
        agregar_columnas_vacias(df_base).to_excel(writer, sheet_name='BASE', index=False)

    return df_base

# Generar hoja IND COURIER
def generar_ind_courier(df_courier, writer) -> None:
    # Obtener los resúmenes por proveedor
    resumenes = generar_tabla_resumen(df_courier)

    # Crear la hoja única
    sheet_name = 'IND COURIER'
    workbook = writer.book
    worksheet = workbook.add_worksheet(sheet_name)

    # Estilos
    formato_titulo = workbook.add_format({'bold': True, 'bg_color': "#D3D3D3"})
    formato_encabezado = workbook.add_format({'bold': True, 'bg_color': "#EFEFEF"})

    # Fila de inicio
    fila_actual = 0

    for proveedor, df_resumen in resumenes.items():
        # Escribir el título del proveedor
        worksheet.write(fila_actual, 0, f"Proveedor: {proveedor}", formato_titulo)
        fila_actual += 1

        # Escribir encabezados de columna
        for col_idx, col_name in enumerate(df_resumen.columns):
            worksheet.write(fila_actual, col_idx, col_name, formato_encabezado)
        fila_actual += 1

        # Escribir los datos del DataFrame
        for row in df_resumen.itertuples(index=False):
            for col_idx, value in enumerate(row):
                worksheet.write(fila_actual, col_idx, value)
            fila_actual += 1

        # Espacio entre proveedores
        fila_actual += 2

    print(f"Hoja '{sheet_name}' generada correctamente con todos los proveedores.")

# Función para generar la hoja MEDIO DE ENVIO
def generar_medio_envio(df_base: pd.DataFrame, workbook) -> None:
    sheet_name = 'MEDIO DE ENVIO'
    worksheet = workbook.add_worksheet(sheet_name)

    formato_titulo = workbook.add_format({'bold': True, 'bg_color': "#D3D3D3"})
    formato_celdas = workbook.add_format({'text_wrap': True, 'valign': 'top'})

    # Escribir encabezado
    worksheet.write('A1', 'Proveedor', formato_titulo)

    # Obtener proveedores únicos de la columna 'Proveedor'
    proveedores = df_base['Proveedor'].dropna().unique()

    # Obtener medios de envío únicos de la columna 'MEDIO DE ENVIO', asegurándonos de capturar todos
    medios_envio = df_base['MEDIO DE ENVIO'].dropna().unique()

    # Escribir los encabezados de los medios de envío
    for i, medio_envio in enumerate(medios_envio, start=1):
        worksheet.write(0, i, medio_envio, formato_titulo)

    # Columna adicional para el Total
    worksheet.write(0, len(medios_envio) + 1, 'Total', formato_titulo)

    startrow = 1  # Empezamos en la fila 2 para los datos

    # Iterar sobre los proveedores
    for proveedor in proveedores:
        df_proveedor = df_base[df_base['Proveedor'] == proveedor]
        if df_proveedor.empty:
            continue

        # Escribir proveedor en la columna A
        worksheet.write(startrow, 0, proveedor, formato_celdas)

        total_proveedor = 0  # Variable para el total del proveedor

        # Calcular los totales por medio de envío
        for i, medio_envio in enumerate(medios_envio, start=1):
            # Filtrar por medio de envío
            df_medio_envio = df_proveedor[df_proveedor['MEDIO DE ENVIO'] == medio_envio]

            # Escribir el total para cada medio de envío
            total_medio = len(df_medio_envio)
            worksheet.write(startrow, i, total_medio, formato_celdas)

            # Sumar al total del proveedor
            total_proveedor += total_medio

        # Escribir el total del proveedor en la columna de "Total"
        worksheet.write(startrow, len(medios_envio) + 1, total_proveedor, formato_celdas)

        startrow += 1  # Incrementar fila

    # Fila de totales
    total_row = startrow
    worksheet.write(total_row, 0, 'Total', formato_titulo)

    # Sumar los totales por cada medio de envío (en la última fila de la tabla)
    for i in range(1, len(medios_envio) + 1):
        worksheet.write_formula(total_row, i, f'SUM({chr(65 + i)}2:{chr(65 + i)}{total_row})', formato_celdas)

    # Sumar los totales verticales (la columna Total)
    worksheet.write_formula(total_row, len(medios_envio) + 1, f'SUM({chr(65 + len(medios_envio) + 1)}2:{chr(65 + len(medios_envio) + 1)}{total_row})', formato_celdas)

    worksheet.autofilter(0, 0, startrow, len(medios_envio) + 1)

# Función para generar la hoja Alerta

def generar_alerta(df_courier: pd.DataFrame, workbook) -> None:
    hoy = datetime.today().date()
    ayer = hoy - timedelta(days=1)
    dia_semana = hoy.weekday()  # lunes = 0, martes = 1, ..., domingo = 6
    festivos = holidays.Colombia(years=[hoy.year])

    # Caso especial: si ayer fue festivo
    if ayer in festivos:
        # Buscar desde el viernes anterior al festivo
        dias = 1
        while True:
            dia = ayer - timedelta(days=dias)
            if dia.weekday() == 4:  # Viernes
                inicio = dia
                break
            dias += 1
        fin = ayer
    elif dia_semana == 0:  # Hoy es lunes
        if hoy in festivos:
            inicio = hoy - timedelta(days=3)  # viernes
            fin = hoy
        else:
            inicio = hoy - timedelta(days=3)  # viernes
            fin = hoy - timedelta(days=1)  # domingo
    else:
        inicio = ayer
        fin = ayer

    print(f"📆 Rango corregido: {inicio} hasta {fin}")

    df_courier['FECHA RADICACION'] = pd.to_datetime(df_courier['FECHA RADICACION'], errors='coerce')
    df_courier['ESTADO GUIA'] = df_courier['ESTADO GUIA'].astype(str).str.strip().str.lower()

    estado_deseado = 'por recibir correspondencia'.lower()

    df_filtrado = df_courier[
        (df_courier['FECHA RADICACION'].dt.date >= inicio) &
        (df_courier['FECHA RADICACION'].dt.date <= fin) &
        (df_courier['ESTADO GUIA'] == estado_deseado)
    ]

    # Crear hoja
    worksheet = workbook.add_worksheet('Alerta')
    formato_titulo = workbook.add_format({'bold': True, 'bg_color': "#D3D3D3"})
    formato_celdas = workbook.add_format({'text_wrap': True, 'valign': 'top'})

    worksheet.write('A1', 'Fecha de Radicación', formato_titulo)
    fecha_actual = inicio
    # Escribir encabezados para cada día dentro del rango
    col = 1
    while fecha_actual <= fin:
        worksheet.write(0, col, f'{fecha_actual}', formato_titulo)
        fecha_actual += timedelta(days=1)
        col += 1
    worksheet.write(0, col, 'Total General', formato_titulo)

    proveedores = df_filtrado['Proveedor'].dropna().unique()
    startrow = 1
    total_general = 0

    for proveedor in proveedores:
        total_por_dia = []
        for dia in range((fin - inicio).days + 1):
            dia_actual = inicio + timedelta(days=dia)
            total_por_dia.append(len(df_filtrado[
                (df_filtrado['Proveedor'] == proveedor) &
                (df_filtrado['FECHA RADICACION'].dt.date == dia_actual)
            ]))
        # Escribir el proveedor y los totales por día
        worksheet.write(startrow, 0, proveedor, formato_celdas)
        for i, total in enumerate(total_por_dia):
            worksheet.write(startrow, i + 1, total, formato_celdas)
        total_general += sum(total_por_dia)
        startrow += 1

    worksheet.write(startrow, 0, 'Total', formato_titulo)
    worksheet.write(startrow, col, total_general, formato_titulo)
    worksheet.autofilter(0, 0, startrow, col)  # Autoajustar el filtro


# Función para crear un gráfico de barras apiladas basado en "FECHA RADICACION", "Proveedor" y "MEDIO DE ENVIO = Mensajero"
def grafico_courier(df_courier, workbook):
    # Asegurar fechas en formato datetime
    df_courier['FECHA RADICACION'] = pd.to_datetime(df_courier['FECHA RADICACION'])

    # Agrupar por fecha (día) y proveedor
    df_agrupado = df_courier.groupby([df_courier['FECHA RADICACION'].dt.date, 'Proveedor']) \
                            .size().reset_index(name='Cantidad')

    # Pivot para gráfico y eliminar valores con cantidad 0
    df_pivot = df_agrupado.pivot(index='FECHA RADICACION', columns='Proveedor', values='Cantidad').fillna(0)

    # Eliminar las columnas con solo ceros
    df_pivot = df_pivot.loc[:, (df_pivot != 0).any(axis=0)]

    # Datos
    fechas = df_pivot.index
    proveedores = df_pivot.columns
    x = np.arange(len(fechas))  # posiciones X

    # Colores personalizados
    colores = ['#809bce', '#95b8d1', "#79cbd1", '#B8E6A7', '#4C9A2A']
    colores_usar = list(islice(cycle(colores), len(proveedores)))

    # Ancho de barra y desplazamiento
    total_width = 0.8
    bar_width = total_width / len(proveedores)

    fig, ax = plt.subplots(figsize=(max(15, len(fechas) * 0.4), 6))

    # Dibujar las barras y agregar los conteos sobre ellas
    for i, prov in enumerate(proveedores):
        bars = ax.bar(x + i * bar_width, df_pivot[prov], width=bar_width, label=prov, color=colores_usar[i])

        # Agregar los conteos sobre cada barra
        for bar in bars:
            yval = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2, yval, int(yval), ha='center', va='bottom', fontsize=8)

    # Ejes y leyenda
    ax.set_xticks(x + total_width / 2 - bar_width / 2)
    ax.set_xticklabels([fecha.strftime('%Y-%m-%d') for fecha in pd.to_datetime(fechas)],
                       rotation=90, ha='center', fontsize=7)

    ax.set_xlabel('Fecha de Radicación')
    ax.set_ylabel('Cantidad')
    ax.set_title('Comportamiento Courier')
    ax.legend(title='Proveedor', bbox_to_anchor=(1.02, 1), loc='upper left')
    plt.tight_layout()

    # Guardar imagen en memoria
    imgdata = BytesIO()
    plt.savefig(imgdata, format='png', dpi=200, transparent=True)
    plt.close()
    imgdata.seek(0)

    # Insertar imagen en hoja de Excel
    sheet_name = 'Comportamiento Courier'
    existing_sheets = [ws.get_name() for ws in workbook.worksheets()]
    if sheet_name not in existing_sheets:
        worksheet = workbook.add_worksheet(sheet_name)
    else:
        worksheet = workbook.get_worksheet_by_name(sheet_name)

    worksheet.insert_image('A1', 'grafico_barras_agrupadas.png', {'image_data': imgdata})

    return workbook


# Función para crear un gráfico de barras apiladas basado en "FECHA RADICACION", "Proveedor" y "MEDIO DE ENVIO = Mensajero"
def generar_grafico_pastel(df_base, workbook):
    # Contar los valores de los diferentes medios de envío
    conteo_medios_envio = df_base['MEDIO DE ENVIO'].value_counts()

    # Filtrar los valores que son mayores que 0 para no mostrarlos en el gráfico
    conteo_medios_envio = conteo_medios_envio[conteo_medios_envio > 0]

    # Colores que se deben usar en el gráfico
    colores = ['#FFB897', '#B8E6A7', '#809bce', "#64a09d", '#CBE6FF']

    # Ajustar el tamaño de la figura en función del número de categorías
    fig_width = len(conteo_medios_envio) * 1.5  # Ajusta el tamaño de la figura para que sea más compacto
    fig_height = 6  # Altura ajustada

    # Función personalizada para mostrar los porcentajes, excluyendo los valores 0
    def formato_porcentaje(pct, allvals):
        absolute = round(pct / 100.*sum(allvals), 0)
        # Solo mostrar porcentajes si son mayores que un umbral (ejemplo 1%)
        if pct > 0.1:  # Mostrar solo si el porcentaje es mayor a 0.1%
            return f"{pct:.1f}%"  # Solo mostrar porcentajes mayores que 0.0
        else:
            return ""  # No mostrar si el porcentaje es 0

    # Crear el gráfico de pastel
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    
    wedges, texts, autotexts = ax.pie(
        conteo_medios_envio,
        labels=None,
        autopct=lambda pct: formato_porcentaje(pct, conteo_medios_envio),
        startangle=90,
        colors=colores,
        pctdistance=1,  # Ajusta la distancia de los porcentajes (puedes experimentar con otros valores)
        textprops={'fontsize': 10},  # Reduce el tamaño de la fuente de los porcentajes
    )

    # Agregar la leyenda fuera del gráfico
    ax.legend(
        wedges,
        conteo_medios_envio.index,
        title="Medios de Envío",
        loc="upper left",  # Coloca la leyenda a la izquierda superior
        title_fontsize='13',
        fontsize='10',
        borderpad=1.5,  # Aumentar espacio entre la leyenda y el gráfico
        bbox_to_anchor=(1.1, 1)  # Mover la leyenda más a la derecha
    )

    # Ajustar el layout para evitar que la leyenda se sobreponga
    plt.subplots_adjust(right=0.75)  # Ajusta el margen derecho para dar espacio a la leyenda

    # Crear un archivo temporal para guardar el gráfico
    with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmpfile:
        img_path = tmpfile.name
        plt.savefig(img_path, bbox_inches='tight', transparent=True)  # Asegura que la leyenda quede dentro de la imagen
        plt.close()

    # Insertar el gráfico en la hoja 'MEDIO DE ENVIO'
    sheet_name = 'MEDIO DE ENVIO'
    worksheet = workbook.get_worksheet_by_name(sheet_name)
    worksheet.insert_image('F2', img_path)

    return workbook


# Tabla Mensajero
def tabla_mensajero(df_base, writer):
    # Filtrar solo los envíos por MENSAJERO
    df_mensajero = df_base[df_base['MEDIO DE ENVIO'].str.lower() == 'mensajero']

    # Agrupar por DESTINATARIO y Proveedor
    tabla = df_mensajero.groupby(['DESTINATARIO_TRANSFORMADO', 'Proveedor']) \
                        .size().unstack(fill_value=0)

    # Añadir fila y columna de totales
    tabla.loc['Total general'] = tabla.sum()
    tabla['Total general'] = tabla.sum(axis=1)

    # Preparar hoja y formato
    workbook = writer.book
    worksheet = workbook.add_worksheet('mensajero')
    writer.sheets['mensajero'] = worksheet

    bold = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3', 'border': 1})
    border = workbook.add_format({'border': 1})
    
    # Formato para color rojo
    rojo = workbook.add_format({'color': 'red', 'border': 1})

    # Diccionario de destinatarios estandarizados para comparación
    destinatarios_validos = {
        'COMPENSAR', 'COLFONDOS', 'SEGUROS BOLIVAR', 'COOMEVA LIQUIDADA', 'JUNTA REGIONAL', 'JUNTA NACIONAL'
    }

    # Encabezado
    worksheet.write(0, 0, 'Destinatario', bold)  # A1
    proveedores = list(tabla.columns[:-1])  # sin el total general

    for col_idx, proveedor in enumerate(proveedores, start=1):
        worksheet.write(0, col_idx, proveedor, bold)  # B1, C1...
    worksheet.write(0, len(proveedores) + 1, 'Total general', bold)

    # Cuerpo
    for row_idx, (destinatario, fila) in enumerate(tabla.iterrows(), start=1):
        # Si la fila es Total general, no marcarla en rojo
        if destinatario == 'Total general':
            worksheet.write(row_idx, 0, destinatario, border)  # Total general sin rojo
        else:
            # Aplicar formato rojo si el destinatario no está en la lista de válidos
            if destinatario not in destinatarios_validos:
                worksheet.write(row_idx, 0, destinatario, rojo)  # Destinatario en rojo
            else:
                worksheet.write(row_idx, 0, destinatario, border)

        for col_idx, valor in enumerate(fila[:-1], start=1):
            worksheet.write(row_idx, col_idx, valor, border)
        
        worksheet.write(row_idx, len(proveedores) + 1, fila['Total general'], border)

    # Ancho columnas
    worksheet.set_column('A:A', 22)
    worksheet.set_column('B:Z', 15)


# Grafico Mensajero Tabla
def grafico_mensajero_tabla(df_base, workbook): 
    # Filtrar solo los registros con MEDIO DE ENVIO == 'Mensajero'
    df_mensajero = df_base[df_base['MEDIO DE ENVIO'].str.lower() == 'mensajero'].copy()

    # Agrupar por DESTINATARIO_TRANSFORMADO y Proveedor
    df_agrupado = df_mensajero.groupby(['DESTINATARIO_TRANSFORMADO', 'Proveedor']) \
                              .size().reset_index(name='Cantidad')

    # Pivot para gráfico
    df_pivot = df_agrupado.pivot(index='DESTINATARIO_TRANSFORMADO', columns='Proveedor', values='Cantidad').fillna(0)

    # Datos para graficar
    destinatarios = df_pivot.index
    proveedores = df_pivot.columns
    x = np.arange(len(destinatarios))  # posiciones X

    # Colores personalizados
    colores = ['#809bce', '#95b8d1', "#79cbd1", '#B8E6A7', '#4C9A2A']
    colores_usar = list(islice(cycle(colores), len(proveedores)))

    # Ancho de barra y desplazamiento
    total_width = 0.8
    bar_width = total_width / len(proveedores)

    # Ajustar el tamaño del gráfico dependiendo de la cantidad de destinatarios y proveedores
    fig_width = max(15, len(destinatarios) * 0.4)  # Ancho ajustable según la cantidad de destinatarios
    fig_height = max(6, len(proveedores) * 0.5)  # Altura ajustable si tienes más proveedores

    # Crear gráfico de barras
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))

    # Dibujar las barras y agregar los conteos sobre ellas
    for i, prov in enumerate(proveedores):
        bars = ax.bar(x + i * bar_width, df_pivot[prov], width=bar_width, label=prov, color=colores_usar[i])

        # Agregar los conteos sobre cada barra
        for bar in bars:
            yval = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2, yval, int(yval), ha='center', va='bottom', fontsize=8)

    # Ejes y leyenda
    ax.set_xticks(x + total_width / 2 - bar_width / 2)
    ax.set_xticklabels(destinatarios, rotation=90, ha='center', fontsize=7)

    ax.set_xlabel('Destinatarios')
    ax.set_ylabel('Cantidad')
    ax.set_title('Desatinatario Mensajero')
    ax.legend(title='Proveedor', bbox_to_anchor=(1.02, 1), loc='upper left')
    plt.tight_layout()

    # Guardar la imagen en memoria
    imgdata = BytesIO()
    plt.savefig(imgdata, format='png', dpi=200, bbox_inches='tight', transparent=True)
    plt.close()
    imgdata.seek(0)

    # Insertar imagen en la hoja de Excel
    sheet_name = 'Desatinatario Mensajero'
    existing_sheets = [ws.get_name() for ws in workbook.worksheets()]
    if sheet_name not in existing_sheets:
        worksheet = workbook.add_worksheet(sheet_name)
    else:
        worksheet = workbook.get_worksheet_by_name(sheet_name)

    worksheet.insert_image('A1', 'grafico_barras_mensajero.png', {'image_data': imgdata})

    return workbook

# Grafico Mensajero Torta
def grafico_mensajero_torta(df_base, workbook):
    # Filtrar solo los registros con MEDIO DE ENVIO == 'Mensajero'
    df_mensajero = df_base[df_base['MEDIO DE ENVIO'].str.lower() == 'mensajero'].copy()

    # Agrupar por Proveedor y contar
    df_torta = df_mensajero.groupby('Proveedor').size().reset_index(name='Cantidad')

    # Datos para la torta
    proveedores = df_torta['Proveedor']
    cantidades = df_torta['Cantidad']

    # Crear gráfico de torta
    fig, ax = plt.subplots(figsize=(8, 8))
    colores = ['#809bce', '#95b8d1', "#79cbd1", '#B8E6A7', '#4C9A2A']
    colores_usar = list(islice(cycle(colores), len(proveedores)))

    wedges, texts, autotexts = ax.pie(
        cantidades,
        labels=proveedores,
        autopct='%1.1f%%',
        startangle=140,
        colors=colores_usar,
        textprops={'fontsize': 8}
    )

    ax.set_title('Desatinatario Mensajero', fontsize=12)
    plt.tight_layout()

    # Guardar imagen en memoria
    imgdata = BytesIO()
    plt.savefig(imgdata, format='png', dpi=200, bbox_inches='tight', transparent=True)
    plt.close()
    imgdata.seek(0)

    # Insertar imagen en Excel
    sheet_name = 'Torta Mensajero'
    existing_sheets = [ws.get_name() for ws in workbook.worksheets()]
    if sheet_name not in existing_sheets:
        worksheet = workbook.add_worksheet(sheet_name)
    else:
        worksheet = workbook.get_worksheet_by_name(sheet_name)

    worksheet.insert_image('A1', 'grafico_torta_mensajero.png', {'image_data': imgdata})

    return workbook

# Generar Excel
def generar_excel(datos: pd.DataFrame) -> bytes:
    output = BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book

        # Llamar a la función para generar la hoja BASE y obtener df_base
        df_base = generar_hoja_base(datos, writer)

        # Transformar destinatarios
        df_base = transformar_destinatarios(df_base)

        # HOJA: Courier (mantenerla tal como estaba antes)
        df_consolidado, df_courier = obtener_dfs_filtrados(datos)
        if not df_courier.empty:
            agregar_columnas_vacias(df_courier).to_excel(writer, sheet_name='Courier', index=False)

            # HOJAS: Un proveedor por hoja
            for nombre_hoja, df_proveedor in obtener_dfs_por_proveedor(df_courier):
                agregar_columnas_vacias(df_proveedor).to_excel(writer, sheet_name=nombre_hoja, index=False)

        # Llamar a la función para generar la hoja ALERTA
        generar_alerta(df_courier, workbook)

        # Generar hoja IND COURIER
        generar_ind_courier(df_courier, writer)

        # Llamar a la función para generar la hoja MEDIO DE ENVIO
        generar_medio_envio(df_base, workbook)

        # Llamar a la función para generar el gráfico de barras apiladas en la hoja "Barras Apiladas"
        grafico_courier(df_courier, workbook)

        # Llamar a la función para generar el gráfico de pastel en la hoja MEDIO DE ENVIO
        generar_grafico_pastel(df_base, workbook)  

        # Tabla mensajero
        tabla_mensajero(df_base, writer)

        # Grafico mensajero Tabla
        grafico_mensajero_tabla(df_base, workbook)

        # Grafico mensajero Torta
        grafico_mensajero_torta(df_base, workbook)

    return output.getvalue()
