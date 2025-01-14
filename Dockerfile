# Usar uma imagem base do Python
FROM python:3.9

# Instalando o git
RUN apt-get update && apt-get install -y git

# Definindo o diretório de trabalho
WORKDIR /app

# Copiando a pasta atual para o container
COPY . /app

# Instalando as dependências
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Executando o script starwars_api.py
CMD ["python", "starwars_api.py"]