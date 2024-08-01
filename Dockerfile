# Usa una imagen base oficial de Python
FROM python:3.9-slim

# Establece el directorio de trabajo en el contenedor
WORKDIR /

# Copia el archivo requirements.txt (si tienes uno) en el directorio de trabajo
# COPY requirements.txt ./

# Instala las dependencias
RUN pip install dash plotly pandas psycopg2-binary sqlalchemy dash-bootstrap-components

# Copia el resto del código de la aplicación en el directorio de trabajo del contenedor
COPY prototype.py .

# Expone el puerto 8050 para la aplicación Dash
EXPOSE 8050

# Establece el comando por defecto para ejecutar la aplicación
CMD ["python", "prototype.py"]
