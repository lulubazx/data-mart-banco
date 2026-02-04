# Usa uma imagem leve do Ruby
FROM ruby:3.0-slim

# Instala dependências do sistema necessárias para compilar gems
RUN apt-get update && apt-get install -y build-essential

# Cria pasta de trabalho
WORKDIR /app

# Instala a biblioteca do Google BigQuery para Ruby
RUN gem install google-cloud-bigquery

# Copia o script para dentro
COPY main.rb .

# O comando padrão ao rodar
CMD ["ruby", "main.rb"]