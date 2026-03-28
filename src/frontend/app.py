import streamlit as st
import requests
import pandas as pd

# --- Configurações Básicas ---
st.set_page_config(page_title="Liane's Smart Library", page_icon="📚", layout="wide")
API_URL = "http://localhost:8000"

# --- Navegação Lateral ---
st.sidebar.title("📚 Liane's Library")
st.sidebar.markdown("Welcome to your smart book tracker!")
page = st.sidebar.radio(
    "Menu", 
    ["Dashboard", "🧠 Smart Assistant", "📖 Book Catalog", "👥 Borrowers", "🔄 Loans"]
)

# --- Página 1: Dashboard ---
if page == "Dashboard":
    st.title("📊 Library Dashboard")
    st.write("Visão geral dos seus empréstimos ativos.")
    
    try:
        response = requests.get(f"{API_URL}/loans/active")
        if response.status_code == 200:
            loans = response.json()
            if loans:
                df = pd.DataFrame(loans)
                # Formata a exibição
                df = df[['transaction_id', 'book_title', 'borrower_name', 'loan_date', 'due_date', 'days_overdue']]
                st.dataframe(df, use_container_width=True)
            else:
                st.success("Nenhum livro emprestado no momento! Todos estão na estante.")
    except requests.exceptions.ConnectionError:
        st.error("⚠️ Erro de conexão. Verifique se a API (FastAPI) está rodando na porta 8000.")

# --- Página 2: AI Smart Assistant ---
elif page == "🧠 Smart Assistant":
    st.title("🧠 Smart Library Assistant")
    st.write("Use a Inteligência Artificial para explorar o seu acervo.")
    
    tab1, tab2 = st.tabs(["✨ Vibe Search", "💬 Chat with Librarian"])
    
    # Aba 1: Vibe Search
    with tab1:
        st.markdown("### Encontre um livro pela *Vibe*")
        st.write("Não lembra o título? Digite sobre o que é a história (ex: 'viagem no espaço', 'magia e escolas').")
        
        query = st.text_input("Descreva o que você quer ler hoje:")
        if st.button("Buscar por Vibe"):
            if query:
                with st.spinner("Analisando as vibes do seu acervo..."):
                    res = requests.get(f"{API_URL}/search/vibe", params={"q": query, "limit": 3})
                    if res.status_code == 200:
                        results = res.json().get("results", [])
                        if results:
                            for r in results:
                                st.info(f"**{r['title']}** por {r['author']}\n\n*Relevância: {r['relevance_score']*100:.1f}%*\n\n{r['content_summary']}")
                        else:
                            st.warning("Nenhum livro combinou com essa vibe.")
                    else:
                        st.error("Erro na busca semântica.")
    
    # Aba 2: Chatbot
    with tab2:
        st.markdown("### Fale com a Bibliotecária Virtual")
        
        # Histórico de chat do Streamlit
        if "messages" not in st.session_state:
            st.session_state.messages = []

        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

        if prompt := st.chat_input("Pergunte algo (ex: Tem livros de aventura?)"):
            st.session_state.messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)

            with st.chat_message("assistant"):
                with st.spinner("Pensando..."):
                    res = requests.post(f"{API_URL}/chat/", json={"message": prompt})
                    if res.status_code == 200:
                        reply = res.json().get("reply", "Desculpe, não entendi.")
                        st.markdown(reply)
                        st.session_state.messages.append({"role": "assistant", "content": reply})
                    else:
                        st.error("Erro ao conectar com o LLM.")

# --- Página 3: Catálogo de Livros ---
elif page == "📖 Book Catalog":
    st.title("📖 Catálogo de Livros")
    
    with st.expander("➕ Adicionar Novo Livro"):
        with st.form("new_book_form"):
            col1, col2 = st.columns(2)
            title = col1.text_input("Título *")
            author = col2.text_input("Autor *")
            isbn = col1.text_input("ISBN")
            cost = col2.number_input("Custo (opcional)", min_value=0.0, step=0.1)
            
            if st.form_submit_button("Salvar Livro"):
                res = requests.post(f"{API_URL}/books/", json={"title": title, "author": author, "isbn": isbn, "cost": cost})
                if res.status_code == 201:
                    st.success(f"Livro '{title}' adicionado!")
                else:
                    st.error("Erro ao adicionar livro.")

    st.markdown("### Acervo Atual")
    res = requests.get(f"{API_URL}/books/")
    if res.status_code == 200:
        books = res.json()
        if books:
            st.dataframe(pd.DataFrame(books)[['book_id', 'title', 'author', 'book_status']], use_container_width=True)

# --- Página 4: Mutuários (Amigos) ---
elif page == "👥 Borrowers":
    st.title("👥 Meus Amigos (Mutuários)")
    
    with st.expander("➕ Adicionar Novo Amigo"):
        with st.form("new_borrower_form"):
            col1, col2 = st.columns(2)
            fname = col1.text_input("Primeiro Nome *")
            lname = col2.text_input("Sobrenome *")
            email = col1.text_input("Email")
            phone = col2.text_input("Telefone")
            
            if st.form_submit_button("Salvar Amigo"):
                res = requests.post(f"{API_URL}/borrowers/", json={"first_name": fname, "last_name": lname, "email": email, "phone_number": phone})
                if res.status_code == 201:
                    st.success("Amigo adicionado com sucesso!")
                else:
                    st.error("Erro ao adicionar amigo.")

    st.markdown("### Lista de Amigos")
    res = requests.get(f"{API_URL}/borrowers/")
    if res.status_code == 200:
        borrowers = res.json()
        if borrowers:
            st.dataframe(pd.DataFrame(borrowers)[['person_id', 'first_name', 'last_name', 'status']], use_container_width=True)

# --- Página 5: Empréstimos ---
elif page == "🔄 Loans":
    st.title("🔄 Controle de Empréstimos")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📤 Emprestar Livro (Checkout)")
        with st.form("checkout_form"):
            b_id = st.number_input("ID do Livro", min_value=1, step=1)
            p_id = st.number_input("ID do Amigo", min_value=1, step=1)
            days = st.number_input("Dias de Empréstimo", value=14, min_value=1)
            
            if st.form_submit_button("Registrar Empréstimo"):
                res = requests.post(f"{API_URL}/loans/", json={"book_id": b_id, "person_id": p_id, "loan_period_days": days})
                if res.status_code == 201:
                    st.success("Empréstimo registrado com sucesso!")
                else:
                    st.error(f"Erro: {res.json().get('detail')}")

    with col2:
        st.subheader("📥 Devolver Livro (Return)")
        with st.form("return_form"):
            t_id = st.number_input("ID da Transação", min_value=1, step=1)
            
            if st.form_submit_button("Registrar Devolução"):
                res = requests.post(f"{API_URL}/loans/{t_id}/return", json={})
                if res.status_code == 200:
                    st.success("Livro devolvido com sucesso!")
                else:
                    st.error(f"Erro: {res.json().get('detail')}")