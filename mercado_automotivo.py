#%% Importação de pacotes

import pandas as pd
import numpy as np
import requests
import ipeadatapy as ip
import matplotlib.pyplot as plt

from factor_analyzer import FactorAnalyzer
from factor_analyzer.factor_analyzer import calculate_bartlett_sphericity
import pingouin as pg
import seaborn as sns
import plotly.io as pio
pio.renderers.default = 'browser'
import plotly.graph_objects as go
import sympy as sy
import scipy as sp

#%% Importação da base

# Salvando o arquivo localmente
file_path = 'SeriesTemporais_Autoveiculos.xlsm'

# Lendo o arquivo XLSM com pandas
df_auto = pd.read_excel(file_path, sheet_name="Séries_Temporais_Autoveículos", engine='openpyxl', skiprows=4)

# Exibindo as primeiras linhas da planilha
print(df_auto.head())

#%% Tratamento da base

# Definindo os nomes para colunas específicas
novo_nome_colunas = {
     "Unnamed: 0": "data",
     'Emplacamento Total': "total.emplac_total",
     'Emplacamento Nacionais': 'total.emplac_nacionais',
     'Emplacamento Importados': 'total.emplac_importados',
     'Produção': 'total.producao',
     'Exportação': 'total.exportacao',
     'Emplacamento Total.1': "auto.emplac_total",
     'Emplacamento Nacionais.1': 'auto.emplac_nacionais',
     'Emplacamento Importados.1': 'auto.emplac_importados',
     'Produção.1': 'auto.producao',
     'Exportação.1': 'auto.exportacao',
     'Emplacamento Total.2': "cml_leves.emplac_total",
     'Emplacamento Nacionais.2': 'cml_leves.emplac_nacionais',
     'Emplacamento Importados.2': 'cml_leves.emplac_importados',
     'Produção.2': 'cml_leves.producao',
     'Exportação.2': 'cml_leves.exportacao',
     'Emplacamento Total.3': "caminhoes.emplac_total",
     'Emplacamento Nacionais.3': 'caminhoes.emplac_nacionais',
     'Emplacamento Importados.3': 'caminhoes.emplac_importados',
     'Produção.3': 'caminhoes.producao',
     'Exportação.3': 'caminhoes.exportacao',
     'Emplacamento Total.4': "onibus.emplac_total",
     'Emplacamento Nacionais.4': 'onibus.emplac_nacionais',
     'Emplacamento Importados.4': 'onibus.emplac_importados',
     'Produção.4': 'onibus.producao',
     'Exportação.4': 'onibus.exportacao'
}

# Renomeando as colunas
df_auto.rename(columns= novo_nome_colunas, inplace= True)

# Criando uma coluna de ano
df_auto['ano'] = df_auto['data'].dt.year

# Eliminando registros maiores que ano de 2024
df_auto = df_auto[df_auto['ano'] <= 2024]

# Removendo a coluna de data
df_auto = df_auto.drop('data', axis=1)

#Agrupando por ano
df_auto = df_auto.groupby('ano').sum().reset_index()

#%% Verificação da base
df_auto.info()

# Remover colunas completamente vazias
df_auto = df_auto.dropna(axis=1, how="all")

# Estatísticas descritivas do dataframe
df_auto.describe()

# Verificação de valores ausentes
missing_values = df_auto.isnull().sum()

#%% Gráficos

# Plotando a evolução dos emplacamentos ao longo do tempo
plt.figure(figsize=(12, 6))
sns.lineplot(x=df_auto["ano"], y=df_auto["total.emplac_total"], label="Emplacamento Total")
sns.lineplot(x=df_auto["ano"], y=df_auto["total.producao"], label="Produção")
plt.xlabel("Ano")
plt.ylabel("Quantidade")
plt.title("Evolução dos Emplacamentos e Produção de Veículos")
plt.legend()
plt.show()


#%% Tratando base de autos

# Aplicando filtro de período
s_df_auto = df_auto[(df_auto['ano'] > 1995) & (df_auto['ano'] < 2024)]

# Agrupamento por ano
s_df_auto = s_df_auto.groupby('ano').agg({'total.emplac_total': sum, 'total.producao': sum, 'total.exportacao': sum})

# Criando medidas descritivas
s_df_auto.to_excel('completo_auto.xlsx', sheet_name='descritivo', index=True)
s_df_auto.describe().to_excel('descritivo_auto.xlsx', sheet_name='descritivo', index=True)


#%% Importação das bases ipea

df_ipca = ip.timeseries('PRECOS12_IPCA12') # IPCA
df_igpm = ip.timeseries('IGP12_IGPM12') # IGP-M
df_selic = ip.timeseries('BM12_TJOVER12') # SELIC
df_pib = ip.timeseries('BM12_PIB12') # PIB
df_dol_v = ip.timeseries('BM_ERV') # Dólar Venda
df_dol_c = ip.timeseries('BM_ERC') # Dólar Compra
df_dsp = ip.timeseries('WEO_DESEMWEOBRA') # Desemprego
df_emplac_tot_atv = ip.timeseries('FENABRAVE12_VENDVETOT12') # Emplacamento total autoveículos mensal até 2024
df_emplac_tot_atm = ip.timeseries('FENABRAVE12_VENDAUTO12') # Emplacamento total automóveis mensal até 2024
df_emplac_nac_atv = ip.timeseries('ANFAVE12_LICVEN12') # Emplacamento nacional autoveículos mensal até 2024
df_prd_atm = ip.timeseries('ANFAVE12_QPASSAM12') # Produção automóveis mensal até 2024
df_prd_atv = ip.timeseries('ANFAVE12_QVETOTM12') # Produção autoveículos mensal até 2024
df_prd_camin = ip.timeseries('ANFAVE12_QCAMINM12') # Produção caminhões mensal até 2024
df_exp_atm = ip.timeseries('ANFAVE12_XPASSAM12') # Exportação automóveis mensal até 2024

#%% Tratamento das bases ipea para bronze

# Definindo schema para os dataframes
schema = {
    "CODE": 'object',
    "DAY": 'int32',
    "MONTH": 'int32',
    "YEAR": 'int32',    
}

# Aplicando schema

b_df_emplac_tot_atm = df_emplac_tot_atm.astype(schema)
b_df_emplac_tot_atv = df_emplac_tot_atv.astype(schema)
b_df_emplac_nac_atv = df_emplac_nac_atv.astype(schema)
b_df_exp_atm = df_exp_atm.astype(schema)
b_df_prd_atm = df_prd_atm.astype(schema)
b_df_prd_atv = df_prd_atv.astype(schema)
b_df_prd_camin = df_prd_camin.astype(schema)
b_df_pib = df_pib.astype(schema)
b_df_selic = df_selic.astype(schema)
b_df_ipca = df_ipca.astype(schema)
b_df_igpm = df_igpm.astype(schema)
b_df_dsp = df_dsp.astype(schema)
b_df_dol_c = df_dol_c.astype(schema)
b_df_dol_v = df_dol_v.astype(schema)

# Definindo tipo de coluna das tabelas
b_df_emplac_tot_atm['VALUE (Unidade)'] = b_df_emplac_tot_atm['VALUE (Unidade)'].astype('int64')
b_df_emplac_tot_atv['VALUE (Unidade)'] = b_df_emplac_tot_atv['VALUE (Unidade)'].astype('int64')
b_df_emplac_nac_atv['VALUE (-)'] = b_df_emplac_nac_atv['VALUE (-)'].astype('int64')
b_df_exp_atm['VALUE (Unidade)'] = b_df_exp_atm['VALUE (Unidade)'].astype('int64')
b_df_prd_atm['VALUE (Unidade)'] = b_df_prd_atm['VALUE (Unidade)'].astype('int64')
b_df_prd_atv['VALUE (Unidade)'] = b_df_prd_atv['VALUE (Unidade)'].astype('int64')
b_df_prd_camin['VALUE (Unidade)'] = b_df_prd_camin['VALUE (Unidade)'].astype('int64')
b_df_pib['VALUE (R$)'] = b_df_pib['VALUE (R$)'].astype('float64')
b_df_selic['VALUE ((% a.m.))'] = b_df_selic['VALUE ((% a.m.))'].astype('float32')
b_df_ipca['VALUE (-)'] = b_df_ipca['VALUE (-)'].astype('float64')
b_df_igpm['VALUE (-)'] = b_df_igpm['VALUE (-)'].astype('float64')
b_df_dsp['VALUE ((%))'] = b_df_dsp['VALUE ((%))'].astype('float32')
b_df_dol_c['VALUE (R$)'] = b_df_dol_c['VALUE (R$)'].astype('float64')
b_df_dol_v['VALUE (R$)'] = b_df_dol_v['VALUE (R$)'].astype('float64')


#%% Tratamento das bases ipea para silver

# Dólar
s_df_dol = b_df_dol_v.merge(b_df_dol_c['VALUE (R$)'], on= 'DATE', how= 'left')
# s_df_dol = s_df_dol.reset_index()
# s_df_dol = s_df_dol[s_df_dol['DATE'] > pd.to_datetime('31/12/1989', format= '%d/%m/%Y')]
s_df_dol = s_df_dol[(s_df_dol['YEAR'] > 1995) & (s_df_dol['YEAR'] < 2024)]
s_df_dol['dolar.cot_ano'] = (s_df_dol['VALUE (R$)_x'] + s_df_dol['VALUE (R$)_y'])/2
s_df_dol = s_df_dol.drop(['CODE', 'RAW DATE', 'DAY', 'MONTH', 'VALUE (R$)_x', 'VALUE (R$)_y'], axis= 1)
s_df_dol = s_df_dol.set_index('YEAR')

# Desemprego
s_df_dsp = b_df_dsp[(b_df_dsp['YEAR'] > 1995) & (b_df_dsp['YEAR'] < 2024)]
s_df_dsp = s_df_dsp.drop(['CODE', 'RAW DATE', 'DAY', 'MONTH'], axis= 1)
s_df_dsp['VALUE ((%))'] = s_df_dsp['VALUE ((%))']/100
s_df_dsp = s_df_dsp.set_index('YEAR')
s_df_dsp = s_df_dsp.rename(columns={'VALUE ((%))': 'dsp.pct_ano'})

# SELIC
s_df_selic = b_df_selic.groupby('YEAR')['VALUE ((% a.m.))'].apply(lambda rates: (rates/100 +1).prod()-1).reset_index()
s_df_selic = s_df_selic[(s_df_selic['YEAR'] > 1995) & (s_df_selic['YEAR'] < 2024)]
s_df_selic = s_df_selic.set_index('YEAR')
s_df_selic = s_df_selic.rename(columns={'VALUE ((% a.m.))': 'selic.pct_ano'})

# Autos
s_df_emplac_tot_atm = b_df_emplac_tot_atm[(b_df_emplac_tot_atm['YEAR'] > 1995) & (b_df_emplac_tot_atm['YEAR'] < 2024)]
s_df_emplac_tot_atm = s_df_emplac_tot_atm.groupby('YEAR')['VALUE (Unidade)'].sum().to_frame()
s_df_emplac_tot_atm = s_df_emplac_tot_atm.rename(columns= {'VALUE (Unidade)':'emplac_atm.un'})

s_df_emplac_tot_atv = b_df_emplac_tot_atv[(b_df_emplac_tot_atv['YEAR'] > 1995) & (b_df_emplac_tot_atv['YEAR'] < 2024)]
s_df_emplac_tot_atv = s_df_emplac_tot_atv.groupby('YEAR')['VALUE (Unidade)'].sum().to_frame()
s_df_emplac_tot_atv = s_df_emplac_tot_atv.rename(columns= {'VALUE (Unidade)':'emplac_atv.un'})
#s_df_emplac_tot_atv = s_df_emplac_tot_atv.sort_index()
#s_df_emplac_tot_atv['emplac_atv.pct_ano'] = (s_df_emplac_tot_atv['emplac_atv.un']/s_df_emplac_tot_atv['emplac_atv.un'].shift(1))-1
#s_df_emplac_tot_atv = s_df_emplac_tot_atv.drop(['emplac_atv.un'], axis= 1)
#s_df_emplac_tot_atv = s_df_emplac_tot_atv.loc[s_df_emplac_tot_atv.index > 1995]

s_df_emplac_nac_atv = b_df_emplac_nac_atv[(b_df_emplac_nac_atv['YEAR'] > 1995) & (b_df_emplac_nac_atv['YEAR'] < 2024)]
s_df_emplac_nac_atv = s_df_emplac_nac_atv.groupby('YEAR')['VALUE (-)'].sum().to_frame()
s_df_emplac_nac_atv = s_df_emplac_nac_atv.rename(columns= {'VALUE (-)':'emplac_nac_atv.un'})

s_df_exp_atm = b_df_exp_atm[(b_df_exp_atm['YEAR'] > 1995) & (b_df_exp_atm['YEAR'] < 2024)]
s_df_exp_atm = s_df_exp_atm.groupby('YEAR')['VALUE (Unidade)'].sum().to_frame()
s_df_exp_atm = s_df_exp_atm.rename(columns= {'VALUE (Unidade)':'exp_atm.un'})
#s_df_exp_atm = s_df_exp_atm.sort_index()
#s_df_exp_atm['exp_atm.pct_ano'] = (s_df_exp_atm['exp_atm.un']/s_df_exp_atm['exp_atm.un'].shift(1))-1
#s_df_exp_atm = s_df_exp_atm.drop(['exp_atm.un'], axis= 1)
#s_df_exp_atm = s_df_exp_atm.loc[s_df_exp_atm.index > 1995]

s_df_prd_atm = b_df_prd_atm[(b_df_prd_atm['YEAR'] > 1995) & (b_df_prd_atm['YEAR'] < 2024)]
s_df_prd_atm = s_df_prd_atm.groupby('YEAR')['VALUE (Unidade)'].sum().to_frame()
s_df_prd_atm = s_df_prd_atm.rename(columns= {'VALUE (Unidade)':'prd_atm.un'})

s_df_prd_atv = b_df_prd_atv[(b_df_prd_atv['YEAR'] > 1995) & (b_df_prd_atv['YEAR'] < 2024)]
s_df_prd_atv = s_df_prd_atv.groupby('YEAR')['VALUE (Unidade)'].sum().to_frame()
s_df_prd_atv = s_df_prd_atv.rename(columns= {'VALUE (Unidade)':'prd_atv.un'})
#s_df_prd_atv = s_df_prd_atv.sort_index()
#s_df_prd_atv['prd_atv.pct_ano'] = (s_df_prd_atv['prd_atv.un']/s_df_prd_atv['prd_atv.un'].shift(1))-1
#s_df_prd_atv = s_df_prd_atv.drop(['prd_atv.un'], axis= 1)
#s_df_prd_atv = s_df_prd_atv.loc[s_df_prd_atv.index > 1995]

s_df_prd_camin = b_df_prd_camin[(b_df_prd_camin['YEAR'] > 1995) & (b_df_prd_camin['YEAR'] < 2024)]
s_df_prd_camin = s_df_prd_camin.groupby('YEAR')['VALUE (Unidade)'].sum().to_frame()
s_df_prd_camin = s_df_prd_camin.rename(columns= {'VALUE (Unidade)':'prd_camin.un'})

# PIB
s_df_pib = b_df_pib[(b_df_pib['YEAR'] > 1993) & (b_df_pib['YEAR'] < 2024)]
s_df_pib = s_df_pib.groupby('YEAR').agg({'VALUE (R$)': 'mean'}) #sum?
s_df_pib = s_df_pib.sort_index()
s_df_pib['pib.pct_ano'] = (s_df_pib['VALUE (R$)']/s_df_pib['VALUE (R$)'].shift(1))-1
s_df_pib = s_df_pib.drop(['VALUE (R$)'], axis= 1)
s_df_pib = s_df_pib.loc[s_df_pib.index > 1995]

# IPCA
s_df_ipca = b_df_ipca.sort_index()
s_df_ipca = s_df_ipca[(s_df_ipca['YEAR'] > 1993) & (s_df_ipca['YEAR'] < 2024)]
s_df_ipca['ipca.pct_ano'] = s_df_ipca['VALUE (-)']/s_df_ipca['VALUE (-)'].shift(1)-1
#s_df_ipca = s_df_ipca.reset_index()
s_df_ipca = s_df_ipca.groupby('YEAR').agg({'ipca.pct_ano': lambda rates: (rates + 1).prod() - 1})
s_df_ipca = s_df_ipca.loc[s_df_ipca.index > 1995]

# IGP-M
s_df_igpm = b_df_igpm.sort_index()
s_df_igpm = s_df_igpm[(s_df_igpm['YEAR'] > 1993) & (s_df_igpm['YEAR'] < 2024)]
s_df_igpm['igpm.pct_ano'] = s_df_igpm['VALUE (-)']/s_df_igpm['VALUE (-)'].shift(1)-1
#s_df_igpm = s_df_igpm.reset_index()
s_df_igpm = s_df_igpm.groupby('YEAR').agg({'igpm.pct_ano': lambda rates: (rates + 1).prod() - 1})
s_df_igpm = s_df_igpm.loc[s_df_igpm.index > 1995]

# Dataframe compilado dos indicadores econômicos
s_df_ind_econ = pd.concat([s_df_dol, s_df_selic, s_df_dsp, s_df_ipca, s_df_igpm, s_df_pib], axis= 1)

#%% Informações sobre as variáveis

# Informações gerais sobre o DataFrame
print(s_df_ind_econ.info())

# Estatísticas descritiva das variáveis
descricao = s_df_ind_econ.describe()
descricao.to_excel('dados_descricao.xlsx', sheet_name='descricao', index=True)
print(s_df_ind_econ.describe())

# Matriz de correlações de Pearson entre as variáveis
pg.rcorr(s_df_ind_econ, method = 'pearson', upper = 'pval', 
         decimals = 4, 
         pval_stars = {0.01: '***', 0.05: '**', 0.10: '*'})
matriz_correlacao = pg.rcorr(s_df_ind_econ, method = 'pearson', upper = 'pval', 
         decimals = 4, 
         pval_stars = {0.01: '***', 0.05: '**', 0.10: '*'})
matriz_correlacao.to_excel('matriz_correlacao.xlsx', sheet_name='matriz', index=True)

# Calculando a matriz de correlação para o gráfico
corr_matrix = s_df_ind_econ.corr()

# Nome das variáveis para o gráfico
novos_nomes_matriz = ['Dólar (R$)','SELIC (% a.a.)','Desemprego (% a.a.)','IPCA (% a.a.)','IGP-M (% a.a.)','PIB (% a.a.)']

# Gráfico da matriz de correlação
plt.figure(figsize=(8, 6))  # Tamanho da figura
ax = sns.heatmap(s_df_ind_econ.corr(), annot=True, cmap='coolwarm', fmt='.2f', cbar=True, 
            linewidths=0.5, vmin=-1, vmax=1, annot_kws={'size': 12}, square= True, 
            xticklabels=novos_nomes_matriz, yticklabels=novos_nomes_matriz)  # Mapa de calor
# Adicionando as anotações manualmente usando matplotlib
for i in range(len(corr_matrix.columns)):
    for j in range(len(corr_matrix.index)):
        ax.text(j + 0.5, i + 0.5, f'{corr_matrix.iloc[i, j]:.2f}', 
                ha='center', va='center', color='black', fontsize=12)
        
plt.title('Matriz de Correlação de Pearson')  # Título do gráfico
plt.savefig('matriz_correlacao.png', dpi= 300, bbox_inches= 'tight')
plt.show()

# Matriz de correlações de Pearson incluindo dados automotivos
s_df_full_correl = pd.concat([s_df_ind_econ, s_df_auto], axis= 1)
matriz_full = pg.rcorr(s_df_full_correl, method = 'pearson', upper = 'pval', 
         decimals = 4, 
         pval_stars = {0.01: '***', 0.05: '**', 0.10: '*'})
matriz_full.to_excel('matriz_full.xlsx', sheet_name='matriz', index=True)

# Teste de Esfericidade de Bartlett
bartlett, p_value = calculate_bartlett_sphericity(s_df_ind_econ)
print(f'Qui² Bartlett: {round(bartlett, 4)}')
print(f'p-valor: {round(p_value, 8)}')

# Definindo a PCA (procedimento inicial com todos os fatores possíveis)
fa = FactorAnalyzer(n_factors=6, method='principal', rotation=None).fit(s_df_ind_econ)

# Obtendo os eigenvalues (autovalores): resultantes da função FactorAnalyzer
autovalores = fa.get_eigenvalues()[0]
print(autovalores) # Temos 6 autovalores, pois são 6 variáveis ao todo

# Soma dos autovalores
round(autovalores.sum(), 2)

#%% Eigenvalues, variâncias e variâncias acumuladas

autovalores_fatores = fa.get_factor_variance()

tabela_eigen = pd.DataFrame(autovalores_fatores)
tabela_eigen.columns = [f"Fator {i+1}" for i, v in enumerate(tabela_eigen.columns)]
tabela_eigen.index = ['Autovalor','Variância', 'Variância Acumulada']
tabela_eigen = tabela_eigen.T
tabela_eigen.to_excel('autovalores.xlsx', sheet_name='autovalores', index=True)
print(tabela_eigen)

# Gráfico da variância acumulada dos componentes principais
plt.figure(figsize=(12,8))
ax = sns.barplot(x=tabela_eigen.index, y=tabela_eigen['Variância'], data=tabela_eigen, palette='rocket')
ax.bar_label(ax.containers[0])
plt.title("Fatores Extraídos", fontsize=16)
plt.xlabel(f"{tabela_eigen.shape[0]} fatores que explicam {round(tabela_eigen['Variância'].sum()*100,2)}% da variância", fontsize=12)
plt.ylabel("Porcentagem de variância explicada", fontsize=12)
plt.show()

#%% Determinando as cargas fatoriais

cargas_fatoriais = fa.loadings_

tabela_cargas = pd.DataFrame(cargas_fatoriais)
tabela_cargas.columns = [f"Fator {i+1}" for i, v in enumerate(tabela_cargas.columns)]
tabela_cargas.index = s_df_ind_econ.columns
tabela_cargas.to_excel('cargas_fatoriais.xlsx', sheet_name='cargas', index=True)
print(tabela_cargas)

# Gráfico das cargas fatoriais (loading plot)
plt.figure(figsize=(12,8))
tabela_cargas_chart = tabela_cargas.reset_index()
nomes_indice = {
    'pib.pct_ano': 'PIB',
    'ipca.pct_ano': 'IPCA',
    'selic.pct_ano': 'SELIC',
    'dolar.cot_ano': 'Dólar',
    'dsp.pct_ano': 'Desemprego',
    'igpm.pct_ano': 'IGP-M'
}

# Renomeando o índice
tabela_cargas_chart['index'] = tabela_cargas_chart['index'].replace(nomes_indice)
plt.scatter(tabela_cargas_chart['Fator 1'], tabela_cargas_chart['Fator 2'], s=50, color='blue')

def label_point(x, y, val, ax):
    a = pd.concat({'x': x, 'y': y, 'val': val}, axis=1)
    for i, point in a.iterrows():
        ax.text(point['x'] + 0.05, point['y'], point['val'])

label_point(x = tabela_cargas_chart['Fator 1'],
            y = tabela_cargas_chart['Fator 2'],
            val = tabela_cargas_chart['index'],
            ax = plt.gca()) 

plt.axhline(y=0, color='grey', ls='--')
plt.axvline(x=0, color='grey', ls='--')
plt.ylim([-1.1,1.1])
plt.xlim([-1.1,1.1])
plt.title("Cargas fatorias dos fatores 1 e 2 para cada macroindicador", fontsize=16)
plt.xlabel(f"Fator 1: {round(tabela_eigen.iloc[0]['Variância']*100,2)}% de variância explicada", fontsize=12)
plt.ylabel(f"Fator 2: {round(tabela_eigen.iloc[1]['Variância']*100,2)}% de variância explicada", fontsize=12)
plt.savefig('fatores.png', dpi= 300, bbox_inches= 'tight')
plt.show()


# Determinando as comunalidades
comunalidades = fa.get_communalities()

tabela_comunalidades = pd.DataFrame(comunalidades)
tabela_comunalidades.columns = ['Comunalidades']
tabela_comunalidades.index = s_df_ind_econ.columns
print(tabela_comunalidades)

#%% Extração dos fatores para as observações do banco de dados

fatores = pd.DataFrame(fa.transform(s_df_ind_econ))
fatores.columns =  [f"Fator {i+1}" for i, v in enumerate(fatores.columns)]

# Adicionando os fatores ao banco de dados
g_df_ind_econ = pd.concat([s_df_ind_econ.reset_index(drop=True), fatores], axis=1)

# Identificando os scores fatoriais
scores = fa.weights_

tabela_scores = pd.DataFrame(scores)
tabela_scores.columns = [f"Fator {i+1}" for i, v in enumerate(tabela_scores.columns)]
tabela_scores.index = s_df_ind_econ.columns
tabela_scores.to_excel('scores_fatoriais.xlsx', sheet_name='score', index=True)
print(tabela_scores)

# Correlação entre os fatores extraídos
# Verificação da correlação entre os fatores igual a zero (ortogonais)
pg.rcorr(g_df_ind_econ[['Fator 1','Fator 2', 'Fator 3', 'Fator 4', 'Fator 5', 'Fator 6']],
         method = 'pearson', upper = 'pval', 
         decimals = 4, 
         pval_stars = {0.01: '***', 0.05: '**', 0.10: '*'})

#%% Critério de Kaiser (raiz latente)

# Verificar os autovalores com valores maiores que 1
# Existem dois componentes maiores do que 1

# Parametrizando a PCA para dois fatores (autovalores > 1)
fa = FactorAnalyzer(n_factors=2, method='principal', rotation=None).fit(s_df_ind_econ)

# Eigenvalues, variâncias e variâncias acumuladas de 2 fatores
autovalores_fatores = fa.get_factor_variance()

tabela_eigen = pd.DataFrame(autovalores_fatores)
tabela_eigen.columns = [f"Fator {i+1}" for i, v in enumerate(tabela_eigen.columns)]
tabela_eigen.index = ['Autovalor','Variância', 'Variância Acumulada']
tabela_eigen = tabela_eigen.T

print(tabela_eigen)

#%% Determinando as cargas fatoriais
cargas_fatoriais = fa.loadings_

tabela_cargas = pd.DataFrame(cargas_fatoriais)
tabela_cargas.columns = [f"Fator {i+1}" for i, v in enumerate(tabela_cargas.columns)]
tabela_cargas.index = s_df_ind_econ.columns
print(tabela_cargas)

# Determinando as novas comunalidades
comunalidades = fa.get_communalities()

tabela_comunalidades = pd.DataFrame(comunalidades)
tabela_comunalidades.columns = ['Comunalidades']
tabela_comunalidades.index = s_df_ind_econ.columns
tabela_comunalidades.to_excel('comunalidades.xlsx', sheet_name='comunalidades', index=True)
print(tabela_comunalidades)

#%% Extração dos fatores para as observações do banco de dados

# Remover os fatores obtidos anteriormente
g_df_ind_econ = g_df_ind_econ.drop(columns=['Fator 1', 'Fator 2', 'Fator 3', 'Fator 4', 'Fator 5', 'Fator 6'])

# Gerando novamente, para os 2 fatores extraídos
fatores = pd.DataFrame(fa.transform(s_df_ind_econ))
fatores.columns =  [f"Fator {i+1}" for i, v in enumerate(fatores.columns)]

# Adicionando os fatores ao banco de dados
g_df_ind_econ = pd.concat([g_df_ind_econ.reset_index(drop=True), fatores], axis=1)

# Identificando os scores fatoriais
scores = fa.weights_

tabela_scores = pd.DataFrame(scores)
tabela_scores.columns = [f"Fator {i+1}" for i, v in enumerate(tabela_scores.columns)]
tabela_scores.index = s_df_ind_econ.columns
tabela_scores.to_excel('scores_fatoriais_2.xlsx', sheet_name='score', index=True)
print(tabela_scores)
#%% Criando um ranking (soma ponderada e ordenamento)

g_df_ind_econ['Ranking'] = 0

for index, item in enumerate(list(tabela_eigen.index)):
    variancia = tabela_eigen.loc[item]['Variância']

    g_df_ind_econ['Ranking'] = g_df_ind_econ['Ranking'] + g_df_ind_econ[tabela_eigen.index[index]]*variancia
    
print(g_df_ind_econ)

# Adição dos dados de produção, exportação, importação e emplacamento no mercado automotivo
g_df_ind_econ.index = range(1996, 2023 + 1)
g_df_ind_econ = pd.concat([g_df_ind_econ, s_df_prd_atv, s_df_exp_atm, s_df_emplac_tot_atv], axis= 1)

# Correlação entre ranking dos fatores com mercado automotivo 
print(pg.rcorr(g_df_ind_econ[['Ranking', 'emplac_atv.un']], 
         method = 'pearson', upper = 'pval', 
         decimals = 4, 
         pval_stars = {0.01: '***', 0.05: '**', 0.10: '*'}))

print(pg.rcorr(g_df_ind_econ[['Ranking', 'exp_atm.un']], 
         method = 'pearson', upper = 'pval', 
         decimals = 4, 
         pval_stars = {0.01: '***', 0.05: '**', 0.10: '*'}))

print(pg.rcorr(g_df_ind_econ[['Ranking', 'prd_atv.un']], 
         method = 'pearson', upper = 'pval', 
         decimals = 4, 
         pval_stars = {0.01: '***', 0.05: '**', 0.10: '*'}))

#%% TESTE

t_df_ind_econ = pd.concat([g_df_ind_econ, s_df_auto], axis= 1)
t_df_ind_econ['Ranking_padrao'] =  (t_df_ind_econ['Ranking'] - t_df_ind_econ['Ranking'].mean())/t_df_ind_econ['Ranking'].std()
t_df_ind_econ['Ranking_zscore'] = sp.stats.zscore(t_df_ind_econ['Ranking'] , ddof= 0)
t_df_ind_econ['Ranking_normalizado'] = (t_df_ind_econ['Ranking']-t_df_ind_econ['Ranking'].min())/(t_df_ind_econ['Ranking'].max()-t_df_ind_econ['Ranking'].min())
    
# Correlação entre ranking dos fatores com mercado automotivo 
print(pg.rcorr(t_df_ind_econ[['Ranking', 'total.emplac_total']], 
         method = 'pearson', upper = 'pval', 
         decimals = 4, 
         pval_stars = {0.01: '***', 0.05: '**', 0.10: '*'}))

print(pg.rcorr(t_df_ind_econ[['Ranking', 'total.exportacao']], 
         method = 'pearson', upper = 'pval', 
         decimals = 4, 
         pval_stars = {0.01: '***', 0.05: '**', 0.10: '*'}))

print(pg.rcorr(t_df_ind_econ[['Ranking', 'total.producao']], 
         method = 'pearson', upper = 'pval', 
         decimals = 4, 
         pval_stars = {0.01: '***', 0.05: '**', 0.10: '*'}))

t_df_ind_econ.to_excel('base_final.xlsx', sheet_name='base', index=True)
s_df_pib.to_excel('pib_verificacao.xlsx', sheet_name='base', index=True)
