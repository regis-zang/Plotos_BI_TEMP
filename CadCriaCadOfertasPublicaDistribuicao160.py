# -*- coding: utf-8 -*-
"""
Fonte de dados:https://dados.cvm.gov.br/dataset/agente_fiduc-cad
Campo: CNPJ_Emissor
Campo: CNPJ_Lider
Campo: Custodiante
Campo: Emissao
Campo: Escriturador
Campo: Gestor
Campo: Nome_Emissor
Campo: Nome_Lider
Campo: Numero_Processo
Campo: Oferta_inicial
Campo: Publico_alvo
Campo: Tipo_Oferta
Campo: Valor_Mobiliario
Campo: Qtde_VM_Instit_Intermed_Partic_Consorcio_Distrib
Campo: Num_Invest_Entidade_Previdencia_Privada
Campo: Data_deliberacao_aprovou_oferta
Campo: Rito_Requerimento
Campo: Num_Invest_Investidor_Estrangeiro
Campo: Identificacao_devedores_coobrigados
Campo: Num_Invest_Demais_Pessoa_Juridica
Campo: Tipo_lastro
Campo: Num_Invest_Demais_Pessoa_Juridica_Emissora_Partic_
Campo: Qtde_VM_Entidade_Previdencia_Privada
Campo: Qtde_VM_Pessoa_Natural
Campo: Endereco_emissor_rede_mundial_computadores
Campo: Destinacao_recursos
Campo: Num_Invest_Clube_Investimento
Campo: Num_Invest_Instit_Financ_Emissora_Partic_Consorcio
Campo: Qtde_VM_Fundos_Investimento
Campo: Qtde_VM_Demais_Instit_Financ
Campo: Bookbuilding
Campo: Qtde_VM_Clube_Investimento
Campo: Qtde_VM_Demais_Pessoa_Juridica_Emissora_Partic_Con
Campo: Data_Encerramento
Campo: Regime_fiduciario
Campo: Num_Invest_Instit_Intermed_Partic_Consorcio_Distri
Campo: Titulo_incentivado
Campo: Tipo_requerimento
Campo: Possibilidade_revolvencia
Campo: Num_Invest_Fundos_Investimento
Campo: Status_Requerimento
    Campo: Agente_fiduciario
Campo: Descricao_garantias
Campo: Data_Registro
Campo: Descricao_lastro
Campo: Num_Invest_Pessoa_Natural
Campo: Valor_Total_Registrado
Campo: Avaliador_Risco
Campo: Qtde_VM_Instit_Financ_Emissora_Partic_Consorcio
Campo: Num_Invest_Soc_Adm_Emp_Prop_Demais_Pess_Jurid_Emis
Campo: Administrador
Campo: Reabertura_serie
Campo: Ativos_alvo
Campo: Qtde_Total_Registrada
Campo: Numero_Requerimento
Campo: Processo_SEI
Campo: Qtde_VM_Demais_Pessoa_Juridica
Campo: Qdte_VM_Soc_Adm_Emp_Prop_Demais_Pess_Jurid_Emiss_P
Campo: Qtde_VM_Investidor_Estrangeiro
Campo: Oferta_vasos_comunicantes
Campo: Num_Invest_Demais_Instit_Financ
Campo: Qtde_VM_Companhia_Seguradora
Campo: Data_requerimento
Campo: Mercado_negociacao
Campo: FIDC_nao_padronizado
Campo: Titulo_padronizado
Campo: Regime_distribuicao
Campo: Grupo_Coordenador
Campo: Tipo_societario
Campo: Num_Invest_Companhia_Seguradora
Campo: Titulo_classificado_como_sustentavel



"""
import pandas as pd
import sqlite3

# Definir as variáveis fornecidas
tbl_destino_carga = 'cvm_cad_ofertas_publica_distribuicao'

# Dicionário de dados com base no arquivo meta
data_dictionary = {
    'CNPJ': {'description': 'Cadastro Nacional de Pessoas Jurídicas', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 20},
    'DENOM_SOCIAL': {'description': 'Nome Social', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 50},
    'DT_REG': {'description': 'Data de Registro', 'domain': 'Alfanumérico', 'data_type': 'date'},
    'Atualizacao_Monetaria': {'description': 'Atualização Monetária', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 50},
    'Classe_Ativo': {'description': 'Classe do ativo', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 100},
    'CNPJ_Emissor': {'description': 'CNPJ do emissor', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 20},
    'CNPJ_Lider': {'description': 'CNPJ do Líder', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 20},
    'CNPJ_Ofertante': {'description': 'CNPJ do Ofertante', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 20},
    'Data_Abertura_Processo': {'description': 'Data de Abertura do Processo Administrativo', 'domain': 'AAAA-MM-DD', 'data_type': 'date', 'size': 10},
    'Data_Dispensa_Oferta': {'description': 'Data em que foi concedido o pedido de dispensa de registro da oferta', 'domain': 'AAAA-MM-DD', 'data_type': 'date', 'size': 10},
    'Data_Emissao': {'description': 'Data de Emissão', 'domain': 'AAAA-MM-DD', 'data_type': 'date', 'size': 10},
    'Data_Encerramento_Oferta': {'description': 'Data em que a oferta foi encerrada', 'domain': 'AAAA-MM-DD', 'data_type': 'date', 'size': 10},
    'Data_Protocolo': {'description': 'Data de Protocolo', 'domain': 'AAAA-MM-DD', 'data_type': 'date', 'size': 10},
    'Data_Registro_Oferta': {'description': 'Data em que foi concedido o pedido de registro da oferta', 'domain': 'AAAA-MM-DD', 'data_type': 'date', 'size': 10},
    'Data_Vencimento': {'description': 'Data de Vencimento', 'domain': 'AAAA-MM-DD', 'data_type': 'date', 'size': 10},
    'Emissao': {'description': 'Número da Emissão', 'domain': 'Numérico', 'data_type': 'varchar', 'size': 30},
    'Especie_Ativo': {'description': 'Espécie do ativo', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 50},
    'Forma_Ativo': {'description': 'Forma do ativo', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 50},
    'Juros': {'description': 'Juros', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 50},
    'Nome_Emissor': {'description': 'Nome do Emissor', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 500},
    'Nome_Lider': {'description': 'Nome do Líder', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 100},
    'Nome_Ofertante': {'description': 'Nome do Ofertante', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 500},
    'Nome_Vendedor': {'description': 'Nome do Vendedor', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 100},
    'Numero_Processo': {'description': 'Número do Processo Administrativo', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 18},
    'Numero_Registro_Oferta': {'description': 'Número de Registro da Oferta', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 59},
    'Oferta_Incentivo_Fiscal': {'description': 'Indica se a oferta é elegível ao incentivo previsto na Lei nº 12.431/11', 'domain': 'S/N', 'data_type': 'varchar', 'size': 1},
    'Oferta_Inicial': {'description': 'Indica se é uma oferta inicial (IPO)', 'domain': 'S/N', 'data_type': 'varchar', 'size': 1},
    'Oferta_Regime_Fiduciario': {'description': 'Indica se os créditos contam com regime fiduciário', 'domain': 'S/N', 'data_type': 'varchar', 'size': 1},
    'Preco_Unitario': {'description': 'Preço Unitário', 'domain': 'Numérico', 'data_type': 'numeric', 'precision': 25, 'scale': 2},
    'Projeto_Audiovisual': {'description': 'Projeto Audiovisual', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 50},
    'Quantidade_No_Lote_Suplementar': {'description': 'Quantidade no Lote Suplementar', 'domain': 'Numérico', 'data_type': 'numeric', 'precision': 19, 'scale': 0},
    'Quantidade_Sem_Lote_Suplementar': {'description': 'Quantidade sem Lote Suplementar', 'domain': 'Numérico', 'data_type': 'numeric', 'precision': 19, 'scale': 0},
    'Quantidade_Total': {'description': 'Quantidade Total', 'domain': 'Numérico', 'data_type': 'numeric', 'precision': 38, 'scale': 2},
    'Rito_Oferta': {'description': 'Rito para a realização da oferta', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 34},
    'Serie': {'description': 'Número da Série', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 30},
    'Tipo_Ativo': {'description': 'Tipo de Ativo', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 150},
    'Tipo_Componente_Oferta_Mista': {'description': 'Tipo do componente de uma oferta mista', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 10},
    'Tipo_Oferta': {'description': 'Tipo da Oferta', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 50},
    'Valor_Total': {'description': 'Valor Total', 'domain': 'Numérico', 'data_type': 'numeric', 'precision': 38, 'scale': 2},
    'Nr_Fundos_Investimento': {'description': '', 'domain': '', 'data_type': 'numeric', 'precision': 5, 'scale': 0},
    'Qtd_Investidor_Estrangeiro': {'description': '', 'domain': '', 'data_type': 'numeric', 'precision': 15, 'scale': 4},
    'Data_Inicio_Oferta': {'description': '', 'domain': '', 'data_type': 'date', 'size': 10},
    'Nr_Pessoa_Fisica': {'description': '', 'domain': '', 'data_type': 'numeric', 'precision': 5, 'scale': 0},
    'Qtd_Cli_Investidor_Estrangeiro': {'description': '', 'domain': '', 'data_type': 'numeric', 'precision': 5, 'scale': 0},
    'QtD_Cli_Demais_Pessoa_Juridica': {'description': '', 'domain': '', 'data_type': 'numeric', 'precision': 5, 'scale': 0},
    'Qtd_Instit_Financ_Emissora_Partic_Consorcio': {'description': '', 'domain': '', 'data_type': 'numeric', 'precision': 15, 'scale': 4},
    # Adicionar os demais campos conforme o metadado
}

# Dados de exemplo para demonstração
data = {
    'Atualizacao_Monetaria': ['Atualizacao Monetaria A', 'Atualizacao Monetaria B'],
    'Classe_Ativo': ['Classe Ativo A', 'Classe Ativo B'],
    'CNPJ_Emissor': ['12345678901234', '98765432109876'],
    'CNPJ_Lider': ['11112222333344', '44443333222211'],
    'CNPJ_Ofertante': ['55556666777788', '88887777665544'],
    'Data_Abertura_Processo': ['2021-01-10', '2021-02-15'],
    'Data_Dispensa_Oferta': ['2021-03-20', '2021-04-25'],
    'Data_Emissao': ['2021-05-30', '2021-06-05'],
    'Data_Encerramento_Oferta': ['2021-07-10', '2021-08-15'],
    'Data_Protocolo': ['2021-09-20', '2021-10-25'],
    'Data_Registro_Oferta': ['2021-11-30', '2021-12-05'],
    'Data_Vencimento': ['2022-01-10', '2022-02-15'],
    'Emissao': ['Emissao A', 'Emissao B'],
    'Especie_Ativo': ['Especie Ativo A', 'Especie Ativo B'],
    'Forma_Ativo': ['Forma Ativo A', 'Forma Ativo B'],
    'Juros': ['Juros A', 'Juros B'],
    'Nome_Emissor': ['Nome Emissor A', 'Nome Emissor B'],
    'Nome_Lider': ['Nome Lider A', 'Nome Lider B'],
    'Nome_Ofertante': ['Nome Ofertante A', 'Nome Ofertante B'],
    'Nome_Vendedor': ['Nome Vendedor A', 'Nome Vendedor B'],
    'Numero_Processo': ['Processo 12345', 'Processo 67890'],
    'Numero_Registro_Oferta': ['Registro Oferta 123', 'Registro Oferta 456'],
    'Oferta_Incentivo_Fiscal': ['S', 'N'],
    'Oferta_Inicial': ['S', 'N'],
    'Oferta_Regime_Fiduciario': ['S', 'N'],
    'Preco_Unitario': [100.50, 75.25],
    'Projeto_Audiovisual': ['Projeto A', 'Projeto B'],
    'Quantidade_No_Lote_Suplementar': [50, 30],
    'Quantidade_Sem_Lote_Suplementar': [100, 75],
    'Quantidade_Total': [150.75, 105.25],
    'Rito_Oferta': ['Rito A', 'Rito B'],
    'Serie': ['Serie A', 'Serie B'],
    'Tipo_Ativo': ['Tipo Ativo A', 'Tipo Ativo B'],
    'Tipo_Componente_Oferta_Mista': ['Componente A', 'Componente B'],
    'Tipo_Oferta': ['Oferta Tipo A', 'Oferta Tipo B'],
    'Valor_Total': [500.25, 750.50],
    'Nr_Fundos_Investimento': [3, 5],
    'Qtd_Investidor_Estrangeiro': [10.25, 15.75],
    'Data_Inicio_Oferta': ['2022-03-10', '2022-04-15'],
    'Nr_Pessoa_Fisica': [2, 4],
    'Qtd_Cli_Investidor_Estrangeiro': [4, 6],
    'QtD_Cli_Demais_Pessoa_Juridica': [8, 10],
    'Qtd_Instit_Financ_Emissora_Partic_Consorcio': [20.75, 25.50],
    # Adicione os demais valores conforme o metadado
}


df = pd.DataFrame(data)

# Conexão com o banco de dados SQLite3 e carregamento dos dados
conn = sqlite3.connect('crowfunding.db')

# Criar a tabela no banco de dados com base no dicionário de dados
create_table_query = f"CREATE TABLE IF NOT EXISTS {tbl_destino_carga} ("

for column, props in data_dictionary.items():
    sql_type = props['data_type']
    if props['data_type'] == 'numeric':
        sql_type += f"({props.get('precision', 10)},{props.get('scale', 0)})"
    elif props['data_type'] == 'varchar' or props['data_type'] == 'char':
        sql_type += f"({props['size']})"

    create_table_query += f"{column} {sql_type}, "

# Remover a última vírgula e espaço extra
create_table_query = create_table_query[:-2] + ")"
conn.execute(create_table_query)

# Carregar os dados do DataFrame na tabela
df.to_sql(tbl_destino_carga, conn, if_exists='replace', index=False)

# Exemplo de consulta para verificar se os dados foram carregados corretamente
cursor = conn.cursor()
cursor.execute(f"SELECT * FROM {tbl_destino_carga}")
rows = cursor.fetchall()
for row in rows:
    print(row)

conn.close()
