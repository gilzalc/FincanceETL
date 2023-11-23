-- create_table.sql
CREATE TABLE IF NOT EXISTS fund_data (
    rentabilidade_24m FLOAT,
    rentabilidade_12m FLOAT,
    volatilidade FLOAT,
    patrimonio_liquido FLOAT,
    preco_da_cota FLOAT,
    valor_inicial_minimo FLOAT,
    taxa_de_administracao FLOAT,
    taxa_de_performance FLOAT,
    razao_social VARCHAR(255),
    cnpj not null NUMERIC(14, 0) PRIMARY KEY
);
