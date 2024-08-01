# Usa una imagen base oficial de Python
FROM python:3.9-slim

# Establece el directorio de trabajo en el contenedor
WORKDIR /app

# Copia el archivo requirements.txt (si tienes uno) en el directorio de trabajo
COPY requirements.txt ./

# Instala las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto del código de la aplicación en el directorio de trabajo del contenedor
COPY . .

# Expone el puerto 8050 para la aplicación Dash
EXPOSE 8050

# Establece el comando por defecto para ejecutar la aplicación
CMD ["python", "app.py"]
