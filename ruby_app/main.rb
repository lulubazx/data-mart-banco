require "google/cloud/bigquery"

# --- CONFIGURACAO ---
PROJECT_ID = ENV.fetch("BIGQUERY_PROJECT_ID")
KEY_FILE = ENV["BIGQUERY_KEYFILE"] || ENV["GOOGLE_APPLICATION_CREDENTIALS"]
ALLOW_PII = ENV["ALLOW_PII"] == "true"

if KEY_FILE && !File.exist?(KEY_FILE)
  raise "BIGQUERY_KEYFILE/GOOGLE_APPLICATION_CREDENTIALS not found at #{KEY_FILE}"
end

def mask_name(name)
  return "" if name.nil?
  parts = name.split
  return name if parts.empty?
  first = parts[0]
  last_initial = parts.length > 1 ? " #{parts[1][0]}." : ""
  "#{first}#{last_initial}"
end

puts " Iniciando Aplicação Ruby (Simulação de API)..."

begin
  # 1. Autenticação
  bigquery = Google::Cloud::Bigquery.new(
    project: PROJECT_ID,
    credentials: KEY_FILE
  )
  puts " Conexão com BigQuery estabelecida!"

  # 2. A Query Ajustada (Para ver dados mesmo com saldo baixo)
  # Removemos o filtro de 50k para ver os dados gerados
  table_name = ENV.fetch("BIGQUERY_TABLE", "bank_mart.kpi_performance_geral")
  sql = "SELECT 
            nome_completo, 
            (total_investido - total_divida_ativa) as saldo_liquido, 
            CASE 
                WHEN total_investido > 50000 THEN 'Alta Renda'
                ELSE 'Varejo'
            END as segmento_cliente 
         FROM `#{table_name}` 
         ORDER BY saldo_liquido DESC
         LIMIT 5"

  puts "\n Buscando TOP 5 Clientes (Geral) para o App..."
  
  # 3. Execução
  results = bigquery.query sql

  puts "\n--- RESPOSTA DA API (JSON) ---"
  results.each do |row|
    nome = ALLOW_PII ? row[:nome_completo] : mask_name(row[:nome_completo])
    puts "{ nome: '#{nome}', saldo: R$ #{row[:saldo_liquido]}, categoria: '#{row[:segmento_cliente]}' }"
  end
  puts "------------------------------"
  puts "\n Integração Ruby <-> BigQuery realizada com sucesso!"

rescue StandardError => e
  puts " Erro na integração Ruby: #{e.message}"
end
