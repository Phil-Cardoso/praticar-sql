# Usar uma imagem base do Python
FROM python:3.9

# Instalando o git
RUN apt-get update && apt-get install -y git

# Clonando o repo
RUN git clone https://github.com/Phil-Cardoso/praticar-sql.git /app

# Definindo o diretório de trabalho
WORKDIR /app

# Instalando as dependências
RUN pip install -r requirements.txt

# Executando o script starwars_api.py
CMD ["python", "starwars_api/starwars_api.py"]