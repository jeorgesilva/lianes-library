import sys
import os
import streamlit as st
import requests
import pandas as pd
from pyzbar.pyzbar import decode
from PIL import Image

# 🚨 A MAGIA TEM QUE ACONTECER ANTES DE QUALQUER IMPORT 'SRC' 🚨
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.ui.styles import apply_styles

# --- Função de Busca da Open Library Embutida ---
def buscar_livro_openlibrary(isbn):
    """Busca os dados do livro usando a API gratuita da Open Library."""
    url = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            key = f"ISBN:{isbn}"
            if key in data:
                book_info = data[key]
                title = book_info.get("title", "Título desconhecido")
                authors_list = book_info.get("authors", [])
                author = authors_list[0]["name"] if authors_list else "Autor desconhecido"
                cover_url = book_info.get("cover", {}).get("large")
                return {"title": title, "author": author, "isbn": isbn, "cover_url": cover_url}
    except Exception as e:
        st.error(f"Erro ao conectar com a Open Library: {e}")
    return None

# --- Configurações Básicas ---
st.set_page_config(page_title="Liane's Smart Library", page_icon="📚", layout="wide")

# Aplica o CSS Premium
apply_styles()

API_URL = "https://lianes-library.onrender.com"

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
                df = df[['transaction_id', 'book_title', 'borrower_name', 'loan_date', 'due_date', 'days_overdue']]
                st.dataframe(df, use_container_width=True)
            else:
                st.success("Nenhum livro emprestado no momento! Todos estão na estante.")
    except requests.exceptions.ConnectionError:
        st.error("⚠️ Erro de conexão. Aguarde um momento enquanto o servidor desperta.")

# --- Página 2: AI Smart Assistant ---
elif page == "🧠 Smart Assistant":
    st.title("🧠 Smart Library Assistant")
    st.write("Use a Inteligência Artificial para explorar o seu acervo.")
    
    tab1, tab2 = st.tabs(["✨ Vibe Search", "💬 Chat with Librarian"])
    
    with tab1:
        st.markdown("### Encontre um livro pela *Vibe*")
        query = st.text_input("Descreva o que você quer ler hoje:")
        if st.button("Buscar por Vibe"):
            if query:
                with st.spinner("Analisando as vibes do seu acervo..."):
                    try:
                        res = requests.get(f"{API_URL}/search/vibe", params={"q": query, "limit": 3})
                        if res.status_code == 200:
                            results = res.json().get("results", [])
                            if results:
                                for book in results:
                                    st.markdown(f"""
                                        <div class="book-card">
                                            <div style="display: flex; justify-content: space-between;">
                                                <strong style="font-size: 1.1rem; color: #F9FAFB;">📖 {book['title']}</strong>
                                                <span class="match-tag">{int(book['relevance_score']*100)}% Vibe Match</span>
                                            </div>
                                            <div style="color: #8B5CF6; font-size: 0.9rem; margin-bottom: 10px; font-weight: 500;">{book['author']}</div>
                                            <div style="font-size: 0.85rem; opacity: 0.8; color: #D1D5DB; line-height: 1.5;">{book['content_summary']}</div>
                                        </div>
                                    """, unsafe_allow_html=True)
                            else:
                                st.warning("Nenhum livro combinou com essa vibe.")
                        else:
                            st.error(f"Erro na busca: {res.text}")
                    except Exception as e:
                        st.error(f"Erro de conexão com o servidor: {e}")
    
    with tab2:
        st.markdown("### Fale com a Bibliotecária Virtual")
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
                    try:
                        res = requests.post(f"{API_URL}/chat/", json={"message": prompt})
                        if res.status_code == 200:
                            reply = res.json().get("reply", "Desculpe, não entendi.")
                            st.markdown(reply)
                            st.session_state.messages.append({"role": "assistant", "content": reply})
                        else:
                            st.error(f"Erro ao conectar com o LLM: {res.text}")
                    except Exception as e:
                        st.error(f"Erro de conexão com o servidor: {e}")

# --- Página 3: Catálogo de Livros ---
elif page == "📖 Book Catalog":
    st.title("📖 Catálogo de Livros")
    tab_scan, tab_manual = st.tabs(["📷 Scan Inteligente", "✍️ Adicionar Manualmente"])
    
    with tab_scan:
        st.markdown("### Escanear Código de Barras (ISBN)")
        st.write("Aponte a câmera para o código de barras na contracapa do livro e tire a foto.")
        foto = st.camera_input("Câmera")
        
        if foto:
            with st.spinner("Analisando a imagem..."):
                imagem = Image.open(foto)
                codigos = decode(imagem)
                
                if codigos:
                    isbn_lido = codigos[0].data.decode("utf-8")
                    st.success(f"✅ Código lido: **{isbn_lido}**")
                    with st.spinner("Buscando dados na Open Library..."):
                        livro = buscar_livro_openlibrary(isbn_lido)
                        if livro:
                            col1, col2 = st.columns([1, 2])
                            with col1:
                                if livro['cover_url']:
                                    st.image(livro['cover_url'], use_container_width=True, caption="Capa Encontrada")
                                else:
                                    st.info("Sem imagem de capa disponível.")
                            with col2:
                                st.markdown(f"### {livro['title']}")
                                st.markdown(f"**Autor:** {livro['author']}")
                                st.markdown(f"**ISBN:** {livro['isbn']}")
                                
                                if st.button("➕ Salvar Livro no Acervo", type="primary"):
                                    payload = {"title": livro['title'], "author": livro['author'], "isbn": livro['isbn'], "cost": 0.0}
                                    res = requests.post(f"{API_URL}/books/", json=payload)
                                    if res.status_code == 201:
                                        st.success(f"O livro '{livro['title']}' foi adicionado com sucesso!")
                                    else:
                                        st.error("Erro ao salvar no banco de dados.")
                        else:
                            st.warning("O código de barras foi lido, mas o livro não está na base da Open Library.")
                else:
                    st.error("Nenhum código de barras detectado. Tente focar melhor e garanta boa iluminação!")

    with tab_manual:
        st.markdown("### Digitação Manual")
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

    st.markdown("---")
    st.markdown("### Acervo Atual")
    try:
        res = requests.get(f"{API_URL}/books/")
        if res.status_code == 200:
            books = res.json()
            if books:
                st.dataframe(pd.DataFrame(books)[['book_id', 'title', 'author', 'book_status']], use_container_width=True)
    except Exception as e:
        st.error(f"Erro ao carregar o catálogo: {e}")

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
    try:
        res = requests.get(f"{API_URL}/borrowers/")
        if res.status_code == 200:
            borrowers = res.json()
            if borrowers:
                st.dataframe(pd.DataFrame(borrowers)[['person_id', 'first_name', 'last_name', 'status']], use_container_width=True)
    except Exception as e:
        st.error(f"Erro ao carregar os amigos: {e}")

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