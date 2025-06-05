import streamlit as st
import pandas as pd
import requests
from openai import OpenAI
import json
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from io import StringIO

# Configuração da página
st.set_page_config(
    page_title="Chatbot Planilha",
    page_icon="📊",
    layout="wide"
)

# Título
st.title("📊 Chatbot da Planilha")
st.markdown("Faça perguntas sobre os dados da planilha e receba respostas inteligentes!")

# Função para autenticar com a API do Google Drive
def authenticate_google_drive():
    """Authenticates with Google Drive API using a service account."""
    print("Tentando autenticar com o Google Drive usando Streamlit Secrets...")
    try:
        # Carregar o conteúdo JSON da chave da conta de serviço dos segredos
        google_drive_key_json = st.secrets["google_drive_key"]

        SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
        # Criar credenciais a partir do conteúdo JSON
        creds = service_account.Credentials.from_service_account_info(
            json.loads(google_drive_key_json), scopes=SCOPES)

        print("Autenticação bem-sucedida.")
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        print(f"Erro na autenticação com o Google Drive usando Secrets: {e}")
        st.error(f"Erro na autenticação com o Google Drive: {e}")
        return None

# Função para carregar dados do Google Drive
@st.cache_data
def load_data_from_drive():
    """Loads data from specified CSV files in a Google Drive folder."""
    print("Iniciando carregamento de dados do Google Drive...")
    drive_service = authenticate_google_drive()
    if not drive_service:
        print("Falha na autenticação, retornando None para dataframes.")
        return None, None

    folder_id = "1vp8z4qSafhgKjqdm2IX5WR7SSiQvYibD"
    header_file_name = "202401_NFs_Cabecalho.csv"
    items_file_name = "202401_NFs_Itens.csv"

    header_df = None
    items_df = None

    try:
        print(f"Buscando arquivos '{header_file_name}' e '{items_file_name}' na pasta {folder_id}...")
        # Buscar os arquivos específicos pelos nomes
        query = f"'{folder_id}' in parents and (name='{header_file_name}' or name='{items_file_name}')"
        results = drive_service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)').execute()
        items = results.get('files', [])

        if not items:
            print("Nenhum arquivo CSV encontrado na pasta especificada.")
            st.warning("Nenhum arquivo CSV encontrado na pasta especificada.")
            return None, None

        print(f"Encontrados {len(items)} arquivos.")
        for item in items:
            file_id = item['id']
            file_name = item['name']
            print(f"Processando arquivo: {file_name} (ID: {file_id})")

            try:
                # Para CSVs puros, usamos files().get_media() em vez de export
                print(f"Tentando baixar arquivo {file_name} com get_media()...")
                request = drive_service.files().get_media(fileId=file_id)
                content_bytes = request.execute()
                content = content_bytes.decode('utf-8')
                print(f"Arquivo {file_name} baixado com sucesso.")
            except Exception as download_error:
                print(f"Não foi possível baixar o arquivo {file_name}: {download_error}")
                st.warning(f"Não foi possível baixar o arquivo {file_name}: {download_error}")
                continue

            # Carregar CSV para DataFrame
            if file_name == header_file_name:
                print(f"Carregando {file_name} para header_df...")
                header_df = pd.read_csv(StringIO(content))
                print(f"{file_name} carregado. Shape: {header_df.shape}")
            elif file_name == items_file_name:
                print(f"Carregando {file_name} para items_df...")
                items_df = pd.read_csv(StringIO(content))
                print(f"{file_name} carregado. Shape: {items_df.shape}")

        if header_df is None or items_df is None:
            print(f"Não foi possível encontrar um ou ambos os arquivos CSV esperados ({header_file_name}, {items_file_name}).")
            st.warning(f"Não foi possível encontrar um ou ambos os arquivos CSV esperados ({header_file_name}, {items_file_name}).")
            return None, None

        print("Ambos os dataframes carregados com sucesso.")
        return header_df, items_df

    except Exception as e:
        print(f"Erro geral ao carregar dados do Google Drive: {e}")
        st.error(f"Erro ao carregar dados do Google Drive: {e}")
        return None, None

# Modificar a função query_groq para usar o novo sistema
def query_ai(question, header_df, items_df):
    try:
        # Carregar a chave da API dos segredos do Streamlit
        openai_api_key = st.secrets["openai"]["api_key"]
        client = OpenAI(api_key=openai_api_key)

        # Função auxiliar para tentar encontrar a coluna de fornecedor
        def find_supplier_column(df):
            # Nomes de colunas comuns para fornecedor
            possible_supplier_cols = ['fornecedor', 'emitente', 'nome fornecedor', 'razao social']
            for col in df.columns:
                for possible_name in possible_supplier_cols:
                    if possible_name in col.lower():
                        # Adicionar verificação adicional para evitar falsos positivos como 'chave do fornecedor'
                        if 'chave' not in col.lower() and 'cnpj' not in col.lower() and 'cpf' not in col.lower():
                            return col
            return None # Retorna None se não encontrar

        # Função auxiliar para tentar encontrar a coluna de chave de acesso
        def find_chave_acesso_column(df):
             # Nomes de colunas comuns para chave de acesso
             possible_chave_cols = ['chave de acesso', 'chaveacesso', 'chave_acesso', 'nfkey']
             for col in df.columns:
                 for possible_name in possible_chave_cols:
                    if possible_name in col.lower():
                        return col
             return None # Retorna None se não encontrar


        prompt_lower = question.lower()
        formatted_result = None # Variável para armazenar resultados pré-calculados

        # --- Lógica de detecção e cálculo para perguntas específicas ---
        if "10 nomes de fornecedores com mais notas fiscais" in prompt_lower or "top 10 fornecedores" in prompt_lower:
            supplier_col = find_supplier_column(header_df)
            # Chave de acesso também deve estar no header para contagem de NF por fornecedor
            chave_acesso_col = find_chave_acesso_column(header_df)

            if supplier_col and chave_acesso_col:
                try:
                    # Contar notas fiscais por fornecedor
                    # Agrupamos pela coluna do fornecedor e contamos as chaves de acesso únicas por fornecedor
                    # Isso garante que estamos contando NFs, não linhas duplicadas no header
                    fornecedores_count = header_df.groupby(supplier_col)[chave_acesso_col].nunique()

                    # Obter os top 10 fornecedores
                    top_10_fornecedores = fornecedores_count.sort_values(ascending=False).head(10)

                    # Formatar o resultado para incluir no prompt da IA como uma string clara
                    formatted_result = "Os 10 fornecedores com mais Notas Fiscais são:\n\n"
                    if not top_10_fornecedores.empty:
                        for fornecedor, count in top_10_fornecedores.items():
                            formatted_result += f"- {fornecedor}: {count} Notas Fiscais\n"
                    else:
                        formatted_result += "Não foram encontrados dados de fornecedores ou notas fiscais para esta análise."


                except Exception as e:
                    # Em caso de erro durante o cálculo, informamos a IA e o usuário
                    formatted_result = f"Erro interno ao calcular os 10 principais fornecedores: {e}"
                    st.error(formatted_result) # Exibe o erro no Streamlit

            else:
                 # Se não encontrar as colunas necessárias, informamos a IA e o usuário
                 formatted_result = "Não foi possível identificar as colunas de fornecedor ou chave de acesso nos dados para calcular o top 10."
                 st.warning(formatted_result) # Exibe o warning no Streamlit
        # --- Fim da lógica de detecção e cálculo ---


        # Função para criar um resumo estatístico dos dados (usada para perguntas gerais)
        def create_data_summary(df, name="Dados"): # Adicionado nome para identificar no prompt
            if df is None: # Lidar com caso onde o dataframe não é necessário
                return f"Nenhum {name} disponível para resumo."
            try:
                # Vamos limitar o resumo para economizar tokens
                summary = {
                    'total_rows': len(df),
                    'columns': list(df.columns),
                    'sample': df.head(5).to_dict('records'), # Limitar amostra a 5 linhas
                    'numeric_summary': df.describe().to_dict() if df.select_dtypes(include=['float64', 'int64']).shape[1] > 0 else {}
                }
                # Retornar uma string formatada
                return f"RESUMO DOS {name.upper()}:\n{json.dumps(summary, indent=2)}"
            except Exception as e:
                 return f"Erro ao criar resumo dos {name}: {e}"


        # Preparar o contexto para a IA
        # Se um resultado formatado foi calculado, use-o no prompt
        # Caso contrário, crie um resumo dos DataFrames relevantes
        if formatted_result:
            # Se um resultado específico foi pré-calculado (como top 10 fornecedores),
            # o prompt incluirá tanto a pergunta quanto o resultado já calculado.
            data_context_for_groq = f"RESULTADO DA ANÁLISE PRÉVIA:\n{formatted_result}"
        else:
            # Para perguntas gerais, crie resumos dos dados disponíveis.
            # analyze_question agora não precisa mais ser chamada aqui,
            # já que a lógica de resumo está na função auxiliar local.
            header_summary_str = create_data_summary(header_df, name="Dados de Cabeçalho")
            items_summary_str = create_data_summary(items_df, name="Dados de Itens")
            data_context_for_groq = f"{header_summary_str}\n\n{items_summary_str}"

        # Remover a chamada original a analyze_question que não é mais necessária aqui
        # analyze_question(question, header_df, items_df)

        # Construir o prompt final para a Groq
        # Ajustamos o prompt para lidar com resultados pré-calculados ou resumos
        prompt_to_groq = f"""
        Você é um assistente especializado em análise de dados. Responda à pergunta do usuário baseado nos dados ou resumos fornecidos.
        \n\n{data_context_for_groq}\n\nPERGUNTA DO USUÁRIO: {question}\n\nInstruções:
        \n- Responda de forma clara e objetiva.\n- Use os dados/resumos fornecidos nos arquivos 202401_NFs_Cabecalho.csv e 202401_NFs_Itens.csv para fundamentar sua resposta.
        \n- Se um resultado específico foi fornecido (como uma lista de top 10), apresente esse resultado de forma amigável e ignore a análise dos dados brutos para essa parte.
        \n- Se a pergunta não puder ser respondida com os dados/resumos disponíveis, informe isso.
        \n- O arquivo 202401_NFs_Cabecalho.csv contém os dados de cabeçalho das notas fiscais e o arquivo 202401_NFs_Itens.csv contém os dados de itens das notas fiscais.
        \n- Se precisar de mais detalhes específicos que não estão no resumo, peça ao usuário para refinar a pergunta ou fornecer mais dados.\n"""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt_to_groq}],
            temperature=0.1,
            max_tokens=1000
        )

        return response.choices[0].message.content
    except Exception as e:
        return f"Erro ao consultar a IA: {e}"

# Carregar dados
# Chamando a nova função para carregar dados do Google Drive
header_df, items_df = load_data_from_drive()

# Verificar se os dados foram carregados com sucesso antes de continuar
if header_df is not None and items_df is not None:
    st.sidebar.header("📋 Dados das Notas Fiscais")
    st.sidebar.write(f"**Total de Notas Fiscais (Cabeçalho):** {len(header_df)}")
    st.sidebar.write(f"**Total de Itens de Notas Fiscais:** {len(items_df)}")
    st.sidebar.write(f"**Colunas Cabeçalho:** {', '.join(header_df.columns)}")
    st.sidebar.write(f"**Colunas Itens:** { ', '.join(items_df.columns)}")

    # Mostrar preview dos dados
    with st.expander("👀 Visualizar dados do Cabeçalho"):
        st.dataframe(header_df)
    with st.expander("👀 Visualizar dados dos Itens"):
        st.dataframe(items_df)

    # Chat interface
    st.header("💬 Chat")

    # Inicializar histórico do chat
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Mostrar histórico do chat
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Input do usuário
    if prompt := st.chat_input("Faça uma pergunta sobre as notas fiscais..."):
        # Adicionar mensagem do usuário ao histórico
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Obter resposta da IA
        with st.chat_message("assistant"):
            with st.spinner("Analisando os dados..."):
                response = query_ai(prompt, header_df, items_df)
                st.markdown(response)

        # Adicionar resposta ao histórico
        st.session_state.messages.append({"role": "assistant", "content": response})

    # Botão para limpar chat
    if st.sidebar.button("🗑️ Limpar Chat"):
        st.session_state.messages = []
        st.rerun()

    # Exemplos de perguntas
    st.sidebar.header("💡 Exemplos de Perguntas")
    example_questions = [
        "Quantas notas fiscais temos?",
        "Qual o valor total dos itens da nota fiscal com CHAVE DE ACESSO X?", # Substituir X por uma chave real nos exemplos
        "Liste os itens da nota fiscal com CHAVE DE ACESSO Y", # Substituir Y por uma chave real
        "Qual a quantidade total de um determinado produto (descreva o produto)?",
        "Qual nota fiscal tem mais itens?"
    ]

    # Adicionar um aviso sobre os exemplos de perguntas
    st.sidebar.markdown("_Para testar perguntas específicas sobre notas fiscais, substitua X ou Y por uma CHAVE DE ACESSO real dos seus dados._")

    for question in example_questions:
        if st.sidebar.button(question, key=f"example_{question}"):
            # Para os botões de exemplo, apenas adicionamos a pergunta ao chat.
            # A resposta será gerada quando o usuário "enviar" a pergunta no chat input.
            # Isso evita chamar a IA duas vezes para a mesma pergunta.
            st.session_state.messages.append({"role": "user", "content": question})
            st.rerun()

else:
    # Mensagem mostrada se os dados não forem carregados
    st.warning("Não foi possível carregar os dados das Notas Fiscais. Verifique as permissões da pasta no Google Drive e o arquivo de chave da conta de serviço.")
    # Limpar histórico do chat se os dados não carregarem para evitar erros
    if "messages" in st.session_state:
        st.session_state.messages = []
