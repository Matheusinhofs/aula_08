
# Para realizar uma ETL (Extract, Transform, Load) simples utilizando Python e a biblioteca Pandas, vamos seguir os seguintes passos:

# Extract: Ler os dados de um arquivo JSON.

# Transform: Concatenar os dados extraídos em um único DataFrame e aplicar uma transformação. A transformação específica dependerá dos dados, mas vamos assumir uma operação simples como um exemplo.

# Load: Salvar o DataFrame resultante em um arquivo CSV ou PARQUET.

# Usando LOG

# Uma função que le e consolida os dados de JSON
import pandas as pd
import os
import glob


def extrair_dados_e_consolidar(pasta:str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(pasta, '*.json'))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list, ignore_index=True)
    return df_total

# uma função para transformar
def calcular_kpi_de_total_de_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df["Total"] = df["Quantidade"] * df["Venda"]
    return df

# uma função que da load em CSV ou parquet ou os dois
def carregar_dados(df: pd.DataFrame, formato_saida: list):
    """
    parametro que vai ser CSV, parquet ou os dois
    """
    for formato in formato_saida:
        if formato == 'csv':
            df.to_csv("dados.csv", index=False)
        if formato == 'parquet':
            df.to_parquet("dados.parquet", index=False)


def pipeline_calcular_kpi_de_vendas_consolidado(pasta: str, formato_de_saida: list):
    data_frame = extrair_dados_e_consolidar(pasta)
    data_frame_calculado = calcular_kpi_de_total_de_vendas(data_frame)
    carregar_dados(data_frame_calculado, formato_de_saida)

# se quiser testar você pode não necessariamente precisa manter na função 
# se um dia esquecer de tirar não vai atrapalhar o módulo
# o __name__ é uma variável que o python já tem por definição, se o método é o principal ele vai ter nome de main - dunder methods

# if __name__ == "__main__":
#     pasta_argumento = 'aula_08\dados'
#     data_frame = extrair_dados_e_consolidar(pasta=pasta_argumento)
#     data_frame_calculado = calcular_kpi_de_total_de_vendas(data_frame)
#     formato_de_saida: list = ['csv','parquet']
#     carregar_dados(data_frame_calculado, formato_de_saida)
