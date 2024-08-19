# Usa una imagen base oficial de Python más completa
FROM python:3.9-buster

# Establece el directorio de trabajo en el contenedor
WORKDIR /app

# Instala las dependencias del sistema necesarias para ODBC Driver
RUN apt-get update && apt-get install -y \
    curl \
    gnupg2 \
    unixodbc \
    apt-transport-https \
    ca-certificates \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update && ACCEPT_EULA=Y apt-get install -y \
    msodbcsql17 \
    unixodbc-dev \
    && apt-get clean -y && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Copia el archivo requirements.txt en el directorio de trabajo
COPY requirements.txt ./

# Instala las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto del código de la aplicación en el directorio de trabajo del contenedor
COPY . .

# Expone el puerto 8050 para la aplicación Dash
EXPOSE 8050

# Establece el comando por defecto para ejecutar la aplicación
CMD ["python", "app.py"]
