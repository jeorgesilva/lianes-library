import sys
import os
import streamlit as st
import requests
import pandas as pd
from pyzbar.pyzbar import decode
from PIL import Image

# adds the parent directory to the system path so we can import our styles module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.ui.styles import apply_styles

# --- Function for searching books in Open Library ---
def buscar_livro_openlibrary(isbn):
    """looks up a book in the Open Library."""
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
        st.error(f"Error: {e}")
    return None

# --- Function for rendering a horizontal book carousel ---
def renderizar_carrossel(titulo_secao, lista_livros):
    """Generates a horizontal book carousel."""
    if not lista_livros:
        return # Não mostra nada se a lista estiver vazia

    # 1. O CSS fixo e reduzido (30% menor) injetado diretamente aqui
    estilo_fixo = """
    <style>
        .nx-row {
            display: flex !important;
            flex-direction: row !important;
            flex-wrap: nowrap !important;
            overflow-x: auto !important;
            gap: 12px !important;
            padding: 20px 0 !important;
            width: 100% !important;
        }
        .nx-row::-webkit-scrollbar { display: none !important; }
        
        .nx-card {
            position: relative !important;
            flex: 0 0 110px !important; /* Largura bem pequena e fixa */
            width: 110px !important;
            height: 165px !important;    /* Proporção vertical */
            background: #1A202C !important;
            border-radius: 4px !important;
            cursor: pointer !important;
            transition: transform 0.3s !important;
            z-index: 1 !important;
        }
        .nx-card:hover { transform: scale(1.1) !important; z-index: 10 !important; }
        
        .nx-cover {
            width: 100% !important;
            height: 100% !important;
            object-fit: cover !important;
            border-radius: 4px !important;
        }
        
        .nx-overlay {
            position: absolute !important;
            bottom: 0; left: 0; right: 0; top: 0;
            background: rgba(0,0,0,0.8) !important;
            opacity: 0 !important;
            transition: opacity 0.3s !important;
            display: flex; flex-direction: column;
            justify-content: center; align-items: center;
            padding: 5px; text-align: center;
        }
        .nx-card:hover .nx-overlay { opacity: 1 !important; }
    </style>
    """
    
    # 2. build the section title and the carousel HTML in a single string to minimize re-renders
    st.markdown(f"{estilo_fixo}<h4 style='margin-bottom: -10px;'>{titulo_secao}</h4>", unsafe_allow_html=True)
    
    html_final = '<div class="nx-row">'
    
    for livro in lista_livros:
        capa = livro.get('cover_url', '')
        titulo = str(livro.get('title', 'no title')).replace("'", " ")
        
        # Define se mostra imagem ou fundo escuro com texto
        if capa and str(capa) != "None":
            conteudo = f'<img src="{capa}" class="nx-cover">'
        else:
            conteudo = f'<div class="nx-cover" style="display:flex;align-items:center;justify-content:center;background:#2D3748;font-size:8px;padding:5px;">{titulo[:15]}</div>'
        
        # Monta o card em uma única string sem espaços (CRÍTICO)
        card_html = f'<div class="nx-card">{conteudo}<div class="nx-overlay"><div style="color:white;font-size:9px;font-weight:bold;">{titulo[:25]}</div><div style="font-size:10px;margin-top:5px;">✔️ 📖 ➕</div></div></div>'
        html_final += card_html
        
    html_final += '</div>'
    
    # 3. Renderiza tudo de uma vez
    st.markdown(html_final, unsafe_allow_html=True)

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

# --- Page 1: Dashboard ---
if page == "Dashboard":
    from src.ui.styles import apply_styles
    apply_styles()
    st.markdown("<h1 style='color: #A855F7; margin-bottom: 20px;'>🍿 Liane's Discovery</h1>", unsafe_allow_html=True)
    
    try:
        # 1. Busca os dados da API (Centralizado aqui para evitar erros)
        res_books = requests.get(f"{API_URL}/books/")
        todos_livros = res_books.json() if res_books.status_code == 200 else []
        
        res_loans = requests.get(f"{API_URL}/loans/active")
        emprestimos_ativos = res_loans.json() if res_loans.status_code == 200 else []

        if not todos_livros:
            st.warning("📭 your shelf is empty. Want to scan some books in the 'Book Catalog'?")
        else:
            # --- Processing Categories ---

            # 1. borrowed books (those that have active loans)
            # Filter the list of all books to find which ones are currently borrowed based on active loans
            ids_ativos = [emp['book_id'] for emp in emprestimos_ativos]
            lista_emprestados = [b for b in todos_livros if b.get('book_id') in ids_ativos]
            if lista_emprestados:
                renderizar_carrossel("⏳ Lent", lista_emprestados)

            # 2. New book (last 10 added based on book_id, assuming it's incremental)
            lista_recentes = sorted(todos_livros, key=lambda x: x.get('book_id', 0), reverse=True)[:10]
            renderizar_carrossel("✨ New Arrivals", lista_recentes)

            # 3. Fantasie (filters books that have fantasy-related keywords in the title or author)
            fantasia = [b for b in todos_livros if any(word in str(b).lower() for word in ["dragon", "magic", "potter", "bruxo", "lenda"])]
            if fantasia:
                renderizar_carrossel("🐉 Epic fantasy", fantasia)

            # 4. SCI-FI E ESPAÇO
            sci_fi = [b for b in todos_livros if any(word in str(b).lower() for word in ["space", "science", "star", "robot", "futuro"])]
            if sci_fi:
                renderizar_carrossel("🚀 Space Travel and Sci-Fi", sci_fi)
            
            # 5. TODO O ACERVO (Para garantir que tudo aparece)
            renderizar_carrossel("📚 Explore the whole library", todos_livros)

    except Exception as e:
        st.error(f"⚠️ error loading the Dashboard: {e}")

# --- Página 2: AI Smart Assistant ---
elif page == "🧠 Smart Assistant":
    st.title("🧠 Smart Library Assistant")
    st.write("Use the power of AI to find books that match your vibe or ask the librarian anything about your collection!")
    
    tab1, tab2 = st.tabs(["✨ Vibe Search", "💬 Chat with Librarian"])
    
    with tab1:
        st.markdown("### Find a book by *Vibe*")
        query = st.text_input("Describe what you want to read today:")
        if st.button("Search by Vibe"):
            if query:
                with st.spinner("Analyzing the vibes of your collection..."):
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
                                st.warning("No books matched that vibe.")
                        else:
                            st.error(f"Error in search: {res.text}")
                    except Exception as e:
                        st.error(f"Connection error with the server: {e}")

    with tab2:
        st.markdown("### Chat with the Virtual Librarian")
        if "messages" not in st.session_state:
            st.session_state.messages = []
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

        if prompt := st.chat_input("Ask something (e.g., Do you have adventure books?)"):
            st.session_state.messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)
            with st.chat_message("assistant"):
                with st.spinner("Thinking..."):
                    try:
                        res = requests.post(f"{API_URL}/chat/", json={"message": prompt})
                        if res.status_code == 200:
                            reply = res.json().get("reply", "Sorry, I didn't understand that.")
                            st.markdown(reply)
                            st.session_state.messages.append({"role": "assistant", "content": reply})
                        else:
                            st.error(f"Error connecting to the LLM: {res.text}")
                    except Exception as e:
                        st.error(f"Error connecting to the server: {e}")

# --- Página 3: Catálogo de Livros ---
elif page == "📖 Book Catalog":
    st.title("📖 Book Catalog")
    
    # Substituímos as Tabs por um Radio horizontal para garantir que a câmera desligue
    modo = st.radio(
        "Choose the addition method:", 
        ["📷 Smart Scan", "✍️ Add Manually"], 
        horizontal=True
    )
    
    # --- MODO 1: SCAN INTELIGENTE ---
    if modo == "📷 Smart Scan":
        st.markdown("### Scan (ISBN)")
        
        # Controle de estado da câmera (Liga/Desliga)
        if "camera on" not in st.session_state:
            st.session_state["camera on"] = False

        # Botões para controlar a câmera
        if not st.session_state["camera on"]:
            if st.button("📸 turn camera on", type="primary"):
                st.session_state["camera on"] = True
                st.rerun() # Recarrega a página para mostrar a câmera
        else:
            if st.button("❌ turn camera off"):
                st.session_state["camera on"] = False
                st.rerun() # Recarrega a página para esconder a câmera

            # A câmera SÓ aparece se o botão "Ligar Câmera" foi clicado
            foto = st.camera_input("Point to the book's barcode")
            
            if foto:
                with st.spinner("Reading..."):
                    imagem = Image.open(foto)
                    codigos = decode(imagem)
                    
                    if codigos:
                        isbn_lido = codigos[0].data.decode("utf-8")
                        st.success(f"✅ Código lido: **{isbn_lido}**")
                        
                        # Opcional: Desligar a câmera automaticamente após ler o código com sucesso
                        # st.session_state["camera on"] = False 
                        
                        with st.spinner("Looking up the book in Open Library..."):
                            livro = buscar_livro_openlibrary(isbn_lido)
                            
                            if livro:
                                col1, col2 = st.columns([1, 2])
                                with col1:
                                    if livro['cover_url']:
                                        st.image(livro['cover_url'], use_container_width=True, caption="Cover Image")
                                    else:
                                        st.info("No cover image available.")
                                
                                with col2:
                                    st.markdown(f"### {livro['title']}")
                                    st.markdown(f"**Author:** {livro['author']}")
                                    st.markdown(f"**ISBN:** {livro['isbn']}")
                                    
                                    if st.button("➕ Save to Collection", type="primary"):
                                        payload = {
                                            "title": livro['title'],
                                            "author": livro['author'],
                                            "isbn": livro['isbn'],
                                            "cost": 0.0,
                                            "cover_url": livro['cover_url']  # <--- ADICIONE ESTA LINHA AQUI!
                                        }
                                        res = requests.post(f"{API_URL}/books/", json=payload)
                                        if res.status_code == 201:
                                            st.success(f"The book '{livro['title']}' was added successfully!")
                                            st.session_state["camera on"] = False # Turns off the camera after saving
                                        else:
                                            st.error("Error saving to the database.")
                            else:
                                st.warning("The barcode was read, but the book is not in the Open Library database.")
                    else:
                        st.error("No barcode detected. Try focusing better and ensure good lighting!")

    # --- MODO 2: ADICIONAR MANUALMENTE ---
    elif modo == "✍️ Add Manually":
        # Garante que a câmera seja esquecida ao mudar de aba
        st.session_state["camera on"] = False 
        
        st.markdown("### Manual Entry")
        with st.form("new_book_form"):
            col1, col2 = st.columns(2)
            title = col1.text_input("Title *")
            author = col2.text_input("Author *")
            isbn = col1.text_input("ISBN")
            cost = col2.number_input("Cost (optional)", min_value=0.0, step=0.1)
            
            if st.form_submit_button("Save Book"):
                res = requests.post(f"{API_URL}/books/", json={"title": title, "author": author, "isbn": isbn, "cost": cost})
                if res.status_code == 201:
                    st.success(f"Book '{title}' added!")
                else:
                    st.error("Error adding book.")

    # --- LISTA DO ACERVO ---
    st.markdown("---")
    st.markdown("### Current Collection")
    try:
        res = requests.get(f"{API_URL}/books/")
        if res.status_code == 200:
            books = res.json()
            if books:
                st.dataframe(pd.DataFrame(books)[['book_id', 'title', 'author', 'book_status']], use_container_width=True)
    except Exception as e:
        st.error(f"Error loading the catalog: {e}")

# --- Página 4: Mutuários (Amigos) ---
elif page == "👥 Borrowers":
    st.title("👥 Contact List")
    with st.expander("➕ Add New Borrower"):
        with st.form("new_borrower_form"):
            col1, col2 = st.columns(2)
            fname = col1.text_input("First Name *")
            lname = col2.text_input("Last Name *")
            email = col1.text_input("Email")
            phone = col2.text_input("Phone")
            if st.form_submit_button("Save Borrower"):
                res = requests.post(f"{API_URL}/borrowers/", json={"first_name": fname, "last_name": lname, "email": email, "phone_number": phone})
                if res.status_code == 201:
                    st.success("Borrower added successfully!")
                else:
                    st.error("Error adding borrower.")

    st.markdown("### Borrower List")
    try:
        res = requests.get(f"{API_URL}/borrowers/")
        if res.status_code == 200:
            borrowers = res.json()
            if borrowers:
                st.dataframe(pd.DataFrame(borrowers)[['person_id', 'first_name', 'last_name', 'status']], use_container_width=True)
    except Exception as e:
        st.error(f"Error loading the borrower list: {e}")

# --- Página 5: Empréstimos ---
elif page == "🔄 Loans":
    st.title("🔄 Loan Management")
    
    # Busca dados em tempo real para traduzir IDs em Nomes e ISBNs
    books_list, friends_list, active_loans = [], [], []
    try:
        books_list = requests.get(f"{API_URL}/books/").json()
        friends_list = requests.get(f"{API_URL}/borrowers/").json()
        active_loans = requests.get(f"{API_URL}/loans/active").json()
    except:
        st.warning("Warning: Failed to load some data from the server.")

    col1, col2 = st.columns(2)
    
    # ==========================================
    # 📤 COLUNA 1: EMPRESTAR LIVRO (CHECKOUT)
    # ==========================================
    with col1:
        st.subheader("📤 Loan")
        
        # Controle da Câmera de Empréstimo
        if "cam_checkout" not in st.session_state: st.session_state.cam_checkout = False
        if "isbn_checkout" not in st.session_state: st.session_state.isbn_checkout = ""

        if not st.session_state.cam_checkout:
            if st.button("📸 Scan Book", key="btn_cam_out"):
                st.session_state.cam_checkout = True
                st.rerun()
        else:
            if st.button("❌ Turn Off Camera", key="btn_close_out"):
                st.session_state.cam_checkout = False
                st.rerun()
            
            foto_out = st.camera_input("Point to the barcode", key="cam_out")
            if foto_out:
                with st.spinner("Reading..."):
                    codigos = decode(Image.open(foto_out))
                    if codigos:
                        st.session_state.isbn_checkout = codigos[0].data.decode("utf-8")
                        st.session_state.cam_checkout = False # Desliga a câmera automaticamente
                        st.rerun()
                    else:
                        st.error("No code detected. Try again with better lighting and focus.")

        # Campos de preenchimento
        st.markdown("---")
        isbn_input_out = st.text_input("ISBN or Book ID", value=st.session_state.isbn_checkout)
        
        # Lista suspensa com os nomes dos amigos em vez de IDs
        opcoes_amigos = {f"{f['first_name']} {f['last_name']}": f['person_id'] for f in friends_list} if friends_list else {"Nenhum amigo cadastrado": None}
        amigo_selecionado = st.selectbox("Select the Borrower", options=list(opcoes_amigos.keys()))
        
        dias = st.number_input("Loan Period (Days)", value=14, min_value=1)
        
        if st.button("Register Loan", type="primary", use_container_width=True):
            b_id = None
            # Tenta descobrir o ID do livro pelo ISBN lido
            for b in books_list:
                if str(b.get('isbn')) == isbn_input_out or str(b.get('book_id')) == isbn_input_out:
                    b_id = b['book_id']
                    break
            
            # Fallback se o utilizador digitou diretamente o ID do livro (ex: "1")
            if not b_id and isbn_input_out.isdigit():
                b_id = int(isbn_input_out)
            
            p_id = opcoes_amigos.get(amigo_selecionado)
            
            if b_id and p_id:
                res = requests.post(f"{API_URL}/loans/", json={"book_id": b_id, "person_id": p_id, "loan_period_days": dias})
                if res.status_code == 201:
                    st.success("Loan registered!")
                    st.session_state.isbn_checkout = "" # Limpa o campo
                else:
                    st.error(f"Error: {res.json().get('detail')}")
            else:
                st.warning("Book not found in inventory or invalid borrower.")

    # ==========================================
    # 📥 COLUNA 2: DEVOLVER LIVRO (RETURN)
    # ==========================================
    with col2:
        st.subheader("📥 Return")
        
        # Controle da Câmera de Devolução
        if "cam_return" not in st.session_state: st.session_state.cam_return = False
        if "isbn_return" not in st.session_state: st.session_state.isbn_return = ""

        if not st.session_state.cam_return:
            if st.button("📸 Scan Return", key="btn_cam_in"):
                st.session_state.cam_return = True
                st.rerun()
        else:
            if st.button("❌ Turn Off Camera", key="btn_close_in"):
                st.session_state.cam_return = False
                st.rerun()
            
            foto_in = st.camera_input("Point to the barcode", key="cam_in")
            if foto_in:
                with st.spinner("Reading..."):
                    codigos = decode(Image.open(foto_in))
                    if codigos:
                        st.session_state.isbn_return = codigos[0].data.decode("utf-8")
                        st.session_state.cam_return = False
                        st.rerun()
                    else:
                        st.error("Code not detected.")

        st.markdown("---")
        isbn_input_in = st.text_input("Book ISBN or Transaction ID", value=st.session_state.isbn_return)
        
        if st.button("Register Return", type="primary", use_container_width=True):
            t_id = None
            
            # Se a pessoa digitou apenas o número da transação (ID curto)
            if isbn_input_in.isdigit() and len(isbn_input_in) < 10: 
                t_id = int(isbn_input_in)
            else:
                # O sistema procura qual é o título do livro que tem esse ISBN
                b_title = next((b['title'] for b in books_list if str(b.get('isbn')) == isbn_input_in), None)
                
                # Procura a transação ativa onde o título bate
                if b_title:
                    for loan in active_loans:
                        if loan.get('book_title') == b_title:
                            t_id = loan.get('transaction_id')
                            break

            if t_id:
                res = requests.post(f"{API_URL}/loans/{t_id}/return", json={})
                if res.status_code == 200:
                    st.success("Book returned successfully!")
                    st.session_state.isbn_return = "" # Limpa o campo
                else:
                    st.error(f"Error: {res.json().get('detail')}")
            else:
                st.warning("No active loan found for this book.")