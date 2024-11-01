import asyncio
import aiomysql
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import plotly.graph_objects as go
import streamlit as st
import pytz


# Configurar a página do Streamlit
st.set_page_config(
    page_title="Relatório de Quebras",
    page_icon=":bar_chart:",
    layout="wide",  # Pode ser "centered" ou "wide"
    initial_sidebar_state="expanded"  # Pode ser "expanded" ou "collapsed"
)

# Aplicar CSS personalizado
st.markdown("""
    <style>
    /* Ajustar a distância da borda superior */
    .main-container {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: flex-start;  /* Certifica que os itens começam do topo */
        background-color: #0E1117;
        color: #FAFAFA;
        padding-top: 0px;  /* Ajuste a distância da borda superior */
    }

    /* Movendo a logo 20px mais para cima */
    .stImage {
        margin-top: -150px; /* Subir a imagem em 20px */
    }

    /* Movendo o título h2 15px mais para cima */
    .stMarkdown h2 {
        margin-top: -150px;  /* Subir o título */
        color: #FAFAFA;
        text-align: center;
    }

    /* Movendo o gauge 50px mais para cima */
    .stPlotlyChart {
        margin-top: -115px; /* Subir o gauge em 50px */
    }

    /* Movendo a tabela 60px mais para cima */
    .dataframe-container {
        margin-top: -80px; /* Subir a tabela em 60px */
        overflow-x: hidden;
    }

    /* Estilo da tabela */
    table {
        width: 80px;
        border-collapse: collapse;
        margin-top: 10px;
    }

    th, td {
        padding: 10px;
        text-align: center;
        border: 1px solid #ddd;
        white-space: nowrap;
        color: #FAFAFA;
    }

    th {
        background-color: #262730;
    }

    td.estado-parada {
        background-color: #FF4B4B;
        color: #FAFAFA;
    }

    td.estado-sem-parada {
        background-color: #97FF4B;
        color: #000000;
    }

    td.estado-em-funcionamento {
        background-color: #FFFF00;
        color: #000000;
    }

    /* Atualização */
    .stMarkdown p {
        text-align: center;
        color: #FAFAFA;
    }

    /* Background da sidebar */
    .css-1d391kg {
        background-color: #262730 !important;
    }

    /* Esconder cabeçalho padrão do Streamlit */
    header, .stApp > header {
        display: none;
    }

    /* Esconder rodapé padrão do Streamlit */
    footer, .stApp > footer {
        display: none;
    }

    </style>
    """, unsafe_allow_html=True)

st.markdown("""
    <style>
    /* Centralizar títulos e valores dos componentes st.metric */
    .css-1urpfgu {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
    }
    /* Centraliza o título e o valor no st.metric */
    .css-1yzk30d p, .css-1yzk30d div {
        text-align: center;
        margin: 0;
    }
    </style>
""", unsafe_allow_html=True)

async def fetch_data():
    # Cria o pool de conexão assíncrono usando as configurações do secrets.toml
    pool = await aiomysql.create_pool(
        host=st.secrets["mysql"]["host"],
        port=int(st.secrets["mysql"]["port"]),
        user=st.secrets["mysql"]["user"],
        password=st.secrets["mysql"]["password"],
        db=st.secrets["mysql"]["database"],
        minsize=st.secrets["mysql"]["minsize"],
        maxsize=st.secrets["mysql"]["maxsize"]
    )

    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:

            # Definir o fuso horário de São Paulo
            sao_paulo_tz = pytz.timezone('America/Sao_Paulo')

            # Calcular o horário de uma hora atrás no fuso horário de São Paulo
            now = datetime.now(sao_paulo_tz)
            one_hour_ago = now - timedelta(hours=1)
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')

            # Query única para combinar todas as informações necessárias
            query = f"""
                SELECT 
                    lrq.cod_usina AS cod_usina,
                    lrq.cod_equipamento AS cod_equipamento,
                    lrq.data_cadastro_previsto AS data_cadastro_previsto,
                    lrq.data_cadastro_quebra AS data_cadastro_quebra,
                    usinas.nome AS nome_usina,
                    eqp.nome AS nome_equipamento,
                    SUM(COALESCE(lc.valor_1, 0) + COALESCE(lc.valor_2, 0) + COALESCE(lc.valor_3, 0) + COALESCE(lc.valor_4, 0) + COALESCE(lc.valor_5, 0)) AS max_value,
                    COUNT(DISTINCT lrq.cod_equipamento) AS count_previsto_quebra,
                    lc.alerta AS alerta,
                    lc.cod_campo AS cod_campo,
                    vp.alerta_80 AS alerta_80,
                    vp.alerta_100 AS alerta_100,
                    vp.previsao AS previsao
                FROM log_relatorio_quebras lrq
                LEFT JOIN sup_geral.usinas usinas ON lrq.cod_usina = usinas.codigo
                LEFT JOIN sup_geral.equipamentos eqp ON lrq.cod_equipamento = eqp.codigo
                LEFT JOIN leituras_consecutivas lc ON lc.cod_equipamento = lrq.cod_equipamento AND lc.cod_campo = 114
                LEFT JOIN valores_previsao vp ON vp.cod_equipamento = lrq.cod_equipamento 
                    AND (ABS(TIMESTAMPDIFF(SECOND, vp.data_cadastro_previsto, lrq.data_cadastro_previsto)) < 300 
                    OR ABS(TIMESTAMPDIFF(SECOND, vp.data_cadastro_previsto, lrq.data_cadastro_quebra)) < 300)
                WHERE DATE(lrq.data_cadastro_previsto) = CURDATE() 
                   OR DATE(lrq.data_cadastro_quebra) = CURDATE()
                GROUP BY lrq.cod_usina, lrq.cod_equipamento
            """

            await cursor.execute(query)
            result = await cursor.fetchall()

            if not result:
                return pd.DataFrame(columns=['estado', 'alerta', 'tipo_alerta', 'nome_usina', 'nome_equipamento', 'cod_equipamento', 'data_cadastro_previsto', 'data_cadastro_quebra']), 0, 100, 0

            # Dicionário para armazenar resultados
            data_dict = {
                'df_log': [],
                'max_value': 0,
                'alerta_count': 0,
                'count_previsto': 0,
            }

            equipamento_alertas = {}

            for row in result:
                cod_usina, cod_equipamento, data_cadastro_previsto, data_cadastro_quebra, nome_usina, nome_equipamento, max_value, count_previsto_quebra, alerta, cod_campo, alerta_80, alerta_100, previsao = row

                # Adicionar dados ao dicionário
                data_dict['df_log'].append({
                    'cod_usina': cod_usina,
                    'cod_equipamento': cod_equipamento,
                    'data_cadastro_previsto': data_cadastro_previsto,
                    'data_cadastro_quebra': data_cadastro_quebra,
                    'nome_usina': nome_usina,
                    'nome_equipamento': nome_equipamento,
                    'estado': 'Parada' if data_cadastro_quebra else 'Sem parada',
                    'alerta': 'Sim' if alerta == 1 else 'Não',
                    'tipo_alerta': None,  # Preenchido posteriormente
                })

                # Acumular valor máximo
                data_dict['max_value'] = max(data_dict['max_value'], max_value if max_value and max_value > 0 else 100)

                # Contagem de alertas
                if alerta == 1 and cod_campo == 114:
                    data_dict['alerta_count'] += 1

                # Verificar e acumular alertas específicos para o equipamento
                if cod_equipamento not in equipamento_alertas:
                    equipamento_alertas[cod_equipamento] = set()

                if alerta_80 == 1:
                    equipamento_alertas[cod_equipamento].add("80%")
                if alerta_100 == 1:
                    equipamento_alertas[cod_equipamento].add("100%")
                if previsao == 1:
                    equipamento_alertas[cod_equipamento].add("Previsão")

            # Atualizar o DataFrame com os tipos de alerta
            for entry in data_dict['df_log']:
                cod_equipamento = entry['cod_equipamento']
                entry['tipo_alerta'] = ', '.join(sorted(equipamento_alertas[cod_equipamento])) if cod_equipamento in equipamento_alertas else None

            # Converter para DataFrame
            df_log = pd.DataFrame(data_dict['df_log'])

            # Adicionar a contagem de `count_previsto`
            data_dict['count_previsto'] = count_previsto_quebra if count_previsto_quebra else 0

            # Ordenar as colunas na ordem desejada, incluindo 'tipo_alerta'
            df_log = df_log[['estado', 'alerta', 'tipo_alerta', 'nome_usina', 'nome_equipamento', 'cod_equipamento', 'data_cadastro_previsto', 'data_cadastro_quebra']]

    pool.close()
    await pool.wait_closed()

    # Retornar os valores de forma segura, com valores padrão se necessário
    return df_log, data_dict['alerta_count'], data_dict['max_value'], data_dict['count_previsto']

import streamlit.components.v1 as components

async def main():
    # Espaços reservados para os elementos que serão atualizados
    placeholder_logo = st.empty()
    placeholder_gauge = st.empty()
    placeholder_table = st.empty()

    # Definir o fuso horário de São Paulo
    tz_sao_paulo = pytz.timezone('America/Sao_Paulo')

    # Adicionar a logo no canto esquerdo
    with placeholder_logo.container():
        st.image('C:/Users/user/Desktop/codigos/Identidade BRG 1 Branco.png', width=200)       #('C:/Users/user/Desktop/codigos/Logo BRG Branco.jpeg') #('C:/Users/user/Desktop/codigos/Identidade-BRG-2-Branco.jpeg')
    #    st.image('../imagens/log_brg_novo_branco_2.png', width=150)
        st.markdown('<div class="main-container">', unsafe_allow_html=True)
        st.markdown('<h2>Relatório de Análise Preditiva Diária</h2>', unsafe_allow_html=True)

    while True:
        # Recuperar os dados
        data, alerta_count, max_value, count_previsto = await fetch_data()
    #    components.iframe("https://app.powerbi.com/view?r=eyJrIjoiNzAyZDQ3NGUtNTY2NC00OGMzLThiMTktZjA2ZjlmNTE0NTc3IiwidCI6ImJmNDE5NTMwLTI1MTItNGVmZi1hZTlkLTQ5YTMyMDFlNzY2MiJ9", width=600, height=373.5)
        
        # Atualizar o gauge
        with placeholder_gauge.container():

            # Obter a hora atual em São Paulo
            now = datetime.now(tz_sao_paulo)
            st.write(f"Atualizado em: {now.strftime('%Y-%m-%d %H:%M:%S')}")

            fig = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=alerta_count,
                title={'text': "Equipamentos com Alerta"},
                gauge={
                    'axis': {'range': [0, max_value]},  # Intervalo do gauge com o valor máximo
                    'bar': {'color': "#FF4B4B"},  # Cor da barra do gauge
                    'bgcolor': "#262730",  # Cor de fundo
                    'steps': [
                        {'range': [0, max_value], 'color': "#262730"}  # Cor de fundo do gauge
                    ]
                },
                number={'font': {'size': 20, 'color': "#FAFAFA"}},  # Cor do texto do número
                delta={'reference': max_value, 'relative': True, 'position': "top"}  # Adiciona uma referência ao valor máximo

            ))
            # Adicionar uma anotação para o valor máximo
            fig.add_annotation(
                x=0.5,
                y=0.5,
                text=f"Máx: {max_value}",
                showarrow=False,
                font=dict(size=14, color="white"),
                align="center",
                xref="paper",
                yref="paper",
                opacity=0.8
            )

            # Ajustar o layout do gráfico para controlar o tamanho
            fig.update_layout(
                autosize=False,
                width=300,  # Ajuste a largura do gráfico
                height=150,  # Ajuste a altura do gráfico
                margin=dict(l=20, r=20, t=20, b=20)
            )

            st.plotly_chart(fig, use_container_width=True)

            # Exibir o número de equipamentos com data de cadastro prevista no dia atual
            st.markdown(f'**Quantidade de alerta diário:**<br>{count_previsto}', unsafe_allow_html=True)


        # Verificar se 'data' é um DataFrame
        if isinstance(data, pd.DataFrame):
            # Renomear as colunas
            data = data.rename(columns={
                'alerta': 'Em Alerta',
                'cod_equipamento': 'Código Equipamento',
                'nome_equipamento': 'Equipamento',
                'nome_usina': 'Usina',
                'data_cadastro_previsto': 'Data Cadastro Previsto',
                'data_cadastro_quebra': 'Data Cadastro Quebra',
                'estado': 'Estado',
                'tipo_alerta': 'Alerta'
            })

            # Atualizar a coluna 'Estado' para 'Em funcionamento' quando 'Alerta' for 'Sim'
            data['Estado'] = data.apply(lambda row: 'Em funcionamento' if row['Em Alerta'] == 'Sim' else row['Estado'], axis=1)

            # Remover as colunas 'Alerta', 'Data Cadastro Previsto', 'Data Cadastro Quebra'
            data = data.drop(columns=['Em Alerta', 'Data Cadastro Previsto', 'Data Cadastro Quebra'])

            # Remover o índice do DataFrame
            data.reset_index(drop=True, inplace=True)

            # Aplicar estilos condicionais
            def apply_styles(df):
                def color_estado(val):
                    if val == 'Parada':
                        return 'background-color: #FF4B4B; color: #FAFAFA;'  # Vermelho
                    elif val == 'Sem parada':
                        return 'background-color: #97FF4B; color: #000000;'  # Verde
                    elif val == 'Em funcionamento':
                        return 'background-color: #FFFF00; color: #000000;'  # Amarelo
                    else:
                        return ''

                # Aplica o estilo de cor de fundo para a coluna 'Estado'
                styled_df = df.style.applymap(color_estado, subset=['Estado'])

                # Centraliza o texto de todas as células
                styled_df = styled_df.set_properties(**{'text-align': 'center'})

                # Centraliza o texto do cabeçalho da tabela
                styled_df = styled_df.set_table_styles([{
                    'selector': 'th',
                    'props': [('text-align', 'center')]
                }])

                return styled_df

            # Atualizar a tabela existente

            with placeholder_table.container():
                st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            #    st.dataframe(apply_styles(data), use_container_width=True)
                st.table(apply_styles(data))
                st.markdown('</div>', unsafe_allow_html=True)


        # Atualiza os dados a cada 1 minuto
        await asyncio.sleep(10)

# Pedro Testes 

async def fetch_alert_ratio(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                SELECT 
                    IFNULL(
                        (SELECT COUNT(*) 
                         FROM falhas_gerais 
                         WHERE falha = 1 
                           AND (alerta_80 = 1 OR alerta_100 = 1 OR previsao = 1)
                        ) 
                        /
                        (SELECT COUNT(*) 
                         FROM falhas_gerais 
                         WHERE falha = 1), 
                    0) * 100 AS Razao_Alertas_Previsao
            """)
            result = await cursor.fetchone()
            return result[0] if result else 0


# Pedro Testes Tempo entre previsão e parada

async def relatorio_geral(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                SELECT 
                    IFNULL(
                        (SELECT COUNT(*) 
                        FROM falhas_gerais 
                        WHERE falha = 1 
                        AND (alerta_80 = 1 OR alerta_100 = 1 OR previsao = 1)
                        ) 
                        /
                        (SELECT COUNT(*) 
                        FROM falhas_gerais 
                        WHERE falha = 1), 
                    0) * 100 AS Razao_Alertas_Previsao,

                    CASE 
                        WHEN AVG(TIMESTAMPDIFF(MINUTE, data_cadastro_previsto, data_cadastro_quebra)) < 60 THEN
                            CONCAT(ROUND(AVG(TIMESTAMPDIFF(MINUTE, data_cadastro_previsto, data_cadastro_quebra)), 0), " minutos")
                        ELSE 
                            CONCAT(
                                FLOOR(AVG(TIMESTAMPDIFF(MINUTE, data_cadastro_previsto, data_cadastro_quebra)) / 60), 
                                ":", 
                                LPAD(ROUND(AVG(TIMESTAMPDIFF(MINUTE, data_cadastro_previsto, data_cadastro_quebra)) % 60, 0), 2, '0'), 
                                " horas"
                            )
                    END AS tempo_medio,

                    (SELECT COUNT(*) 
                    FROM log_relatorio_quebras 
                    WHERE data_cadastro_quebra IS NOT NULL 
                    AND data_cadastro_previsto IS NOT NULL
                    ) AS Contagem_Log_Relatorio_Quebras,

                    (SELECT COUNT(id)  -- Conta apenas as linhas onde id não é nulo
                    FROM machine_learning.coeficiente_geradores
                    ) AS Analise_preditiva
                FROM 
                    log_relatorio_quebras
                LIMIT 1;
            """)
            result = await cursor.fetchone()
            if result:
                return {
                    "razao_alertas_previsao": result[0],
                    "tempo_medio": result[1],
                    "contagem_log_relatorio": result[2],
                    "analise_preditiva": result[3]
                }
            return None

async def Total_Paradas(pool):
    async with pool.acquire()as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("""



                    """)
            result = await cursor.fetchone()
            return result[0] if result else "N/A"

async def BI_ST2():
    # Cria o pool de conexão assíncrono usando as configurações do secrets.toml
    pool = await aiomysql.create_pool(
        host=st.secrets["mysql"]["host"],
        port=int(st.secrets["mysql"]["port"]),
        user=st.secrets["mysql"]["user"],
        password=st.secrets["mysql"]["password"],
        db=st.secrets["mysql"]["database"],
        minsize=st.secrets["mysql"]["minsize"],
        maxsize=st.secrets["mysql"]["maxsize"]
    )

    # Executa as consultas e armazena os resultados
    razao_alertas_previsao = await relatorio_geral(pool)
    previsao_antes_falha = await relatorio_geral(pool)

    # Exibe os resultados no Streamlit
    st.title("Relatório Geral de Previsões")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(label="Razão de Falhas e Previsão", value=f"{razao_alertas_previsao['razao_alertas_previsao']:.2f}%")
    with col2:
        st.metric(label="Tempo entre previsão e parada", value=razao_alertas_previsao['tempo_medio'])
    with col3:
        st.metric(label="Previsões Antes de Quebra", value=f"{int(razao_alertas_previsao['contagem_log_relatorio'])} Equipamentos")
    with col4:
        st.metric(label="Análise Preditiva Ativa", value=f"{int(razao_alertas_previsao['analise_preditiva'])} Equipamentos")


    # Fecha o pool após uso
    pool.close()
    await pool.wait_closed()


async def run_async_functions():
    await asyncio.gather(main(), BI_ST2())

# Use `asyncio.run` apenas se estiver fora do ambiente Streamlit
if __name__ == "__main__":
    asyncio.run(run_async_functions())
    



'''
MeuRH01|C012LC_PROD|https://transbraz179646.protheus.cloudtotvs.com.br:4020/meurh01/?restPort=4050||https://transbraz179646.protheus.cloudtotvs.com.br:4050/restmeurh01|

'''



