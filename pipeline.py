from etl import pipeline_calcular_kpi_de_vendas_consolidado

formato_de_saida: list = ['csv','parquet']
pasta_argumento = 'aula_08\dados'

pipeline_calcular_kpi_de_vendas_consolidado(pasta_argumento, formato_de_saida)